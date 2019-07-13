import signal
from asyncio import create_task
from dataclasses import dataclass, field

import asyncio
import functools
import itertools
import json
import os
import pprint
import resource
import threading
import urllib.parse
from datetime import datetime, timedelta
from enum import Enum
from time import sleep
from typing import Dict, List, Callable, Iterable

import requests
import uvicorn
import uvloop
from werkzeug.utils import cached_property
import logging

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def Now(): return datetime.now()


@dataclass
class Notification:
    recipient: str
    message: str


class DeliveryAttempt:
    def __init__(self, start_time: datetime, notification: Notification, action: Callable):
        self.notification = notification
        self.action = action
        self.start_time = start_time
        self.end_time = None
        self.success = None
        self.running = None

    async def run(self):
        self.running = True
        self.start_time = Now()
        try:
            self.action()
            self.success = True
        except Exception as e:
            self.exception = e
            self.success = False
        finally:
            self.running = False
            self.end_time = Now()

    @property
    def finished(self):
        return self.running is False

    @property
    def never_ran(self):
        return self.running is None

    def __repr__(self):
        return f"Attempt-{id(self)} for Ntfn-{id(self.notification)}, running={self.running}, success={self.success}"


class DeliveryActionFactory:  # Interface
    def get_delivery_command(self, notification: Notification):
        raise NotImplementedError()

    def status_summary(self):
        raise NotImplementedError()


@dataclass()
class HttpPostDeliveryConfig(object):
    url: str
    timeout: timedelta


class HttpPostDeliveryMethod(DeliveryActionFactory):
    def __init__(self, config: HttpPostDeliveryConfig):
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        self.counter = 0

    def post_notification(self, notification: Notification):
        url = self.config.url
        body = str(notification)
        self.log.debug(f"Notification {id(notification)} --> {url}")
        requests.post(url, data=body, timeout=self.config.timeout.total_seconds())
        self.counter += 1

    def get_delivery_command(self, notification: Notification):
        def command():
            self.post_notification(notification)

        return command

    def status_summary(self):
        return {'sent': self.counter}


class NotificationState(Enum):
    QUEUED = 1
    PROCESSING = 2
    DONE = 3
    FAILED = 5


@dataclass
class NotificationRecord:
    notification: Notification
    added_at: datetime
    state: NotificationState = NotificationState.QUEUED
    attempts: List[DeliveryAttempt] = field(default_factory=list)


class NotificationStorage:

    def __init__(self):
        self._records: Dict[id, NotificationRecord] = {}
        self._archive: List[Notification] = []
        self.log = logging.getLogger(self.__class__.__name__)
        self._n_archived = 0

    def add_notification(self, n: Notification):
        idn = id(n)
        self.log.debug(f"Requested to add notification with id={idn} to the storage")
        if idn not in self._records:
            self._records[idn] = NotificationRecord(notification=n, added_at=Now())
            self.log.debug(f"Added notification with id={idn} to the storage")

    def pick_for_delivery(self, notification: Notification):
        self._records[id(notification)].state = NotificationState.PROCESSING
        self.log.info(f"Set notification {id(notification)} as PROCESSING")

    def set_delivered(self, notification: Notification):
        self._records[id(notification)].state = NotificationState.DONE
        self.log.info(f"Set notification {id(notification)} as DONE")

    def register_attempt(self, attempt: DeliveryAttempt):
        self._records[id(attempt.notification)].attempts.append(attempt)

    def last_attempt_for(self, notification: Notification) -> DeliveryAttempt:
        return self._records[id(notification)].attempts[-1]

    def get_notifications_for(self, recipient: str) -> Iterable[Notification]:
        return [r.notification for r in self._records.values() if r.notification.recipient == recipient]

    def get_queued_notifications(self) -> Iterable[Notification]:  # Sync me
        return (r.notification for r in self._records.values() if r.state == NotificationState.QUEUED)

    def get_notifications_processing(self) -> Iterable[Notification]:  # Sync me
        return (r.notification for r in self._records.values() if r.state == NotificationState.PROCESSING)

    def unprocessed_from(self, start_time: datetime) -> Iterable[Notification]:
        return (r.notification for r in self._records.values() if
                r.state == NotificationState.PROCESSING and
                len(r.attempts) > 0 and
                r.attempts[-1].finished and
                r.attempts[-1].success is False and
                r.added_at > start_time)

    def processed_from(self, long_ago: datetime) -> Iterable[Notification]:
        return (r.notification for r in self._records.values() if
                r.state in (NotificationState.DONE, NotificationState.FAILED) and
                r.added_at < long_ago)

    def status_summary(self):
        return {
            'total': len(self._records),
            'queued': sum(1 for r in self._records.values() if r.state == NotificationState.QUEUED),
            'processing': sum(1 for r in self._records.values() if r.state == NotificationState.PROCESSING),
            'done': sum(1 for r in self._records.values() if r.state == NotificationState.DONE),
            'archived': self._n_archived,
        }

    def archive_notification(self, ntf):
        self.log.debug(f"Archiving notification {id(ntf)}")
        # self._archive.append(ntf)
        self._n_archived += 1
        del self._records[id(ntf)]


@dataclass
class SchedulerConfig(object):
    retry_interval: timedelta
    fail_delivery_after: timedelta
    active_delivery_queue_size: int


@dataclass
class RetentionConfig(object):
    check_interval: timedelta
    archive_notifications_after: timedelta


class NotificationRetentionManager:
    def __init__(self, config: RetentionConfig, storage: NotificationStorage):
        self._config = config
        self._storage = storage
        self.log = logging.getLogger(self.__class__.__name__)
        self.stopped = threading.Event()

    def run(self):
        self.log.info(f"Running retention policy with config {self._config}")
        while not self.stopped.is_set():
            self.log.info("Running archive routine")
            for ntf in tuple(self._storage.processed_from(Now() - self._config.archive_notifications_after)):
                self._storage.archive_notification(ntf)
            self.stopped.wait(self._config.check_interval.total_seconds())

    def __call__(self):
        self.run()

    def stop(self):
        self.log.info("Stopping retention management")
        self.stopped.set()


class NotificationDeliveryScheduler:
    def __init__(self,
                 config: SchedulerConfig,
                 notification_storage: NotificationStorage,
                 delivery_action_factory: DeliveryActionFactory
                 ):
        self._action_factory = delivery_action_factory
        self._config = config
        self._notification_storage: NotificationStorage = notification_storage
        self._delivery_jobs: Dict[id, DeliveryAttempt] = {}
        self._stopped = threading.Event()
        self.log = logging.getLogger(self.__class__.__name__)

    async def run(self):
        self.log.info(f'Starting main schedule cycle with {self._config}')
        while not self._stopped.is_set():
            try:
                await self._create_delivery_attempts()
                await self._run_delivery_attempts()
                self._register_attempts()
                self.log.debug(f"Sleeping for {self._config.retry_interval} seconds")
                await asyncio.sleep(self._config.retry_interval.total_seconds())
            except Exception as e:
                self.log.exception("Exception in scheduler:", exc_info=e)

    async def _create_delivery_attempts(self):
        notifications_to_deliver = tuple(itertools.chain(self._notification_storage.get_queued_notifications(),
                                                         self._notification_storage.unprocessed_from(
                                                             Now() - self._config.fail_delivery_after)))

        n_attempts_to_create = self._config.active_delivery_queue_size - len(self._delivery_jobs)
        self.log.debug(f"Creating {n_attempts_to_create} attempts to deliver")
        for ntf in notifications_to_deliver:
            idn = id(ntf)
            if idn not in self._delivery_jobs:
                n_attempts_to_create -= 1
                if n_attempts_to_create < 1: break
                self.log.debug(f"Picking up notification {idn}={ntf}")
                self._notification_storage.pick_for_delivery(ntf)
                self.log.debug(f"Delivery job for notification {idn} doesn't exist, creating...")
                attempt = DeliveryAttempt(Now(), ntf, self._action_factory.get_delivery_command(ntf))
                self._delivery_jobs[idn] = attempt
                self.log.debug(f'Created delivery attempt {id(attempt)} for notification {idn}')

    async def _run_delivery_attempts(self):  # should be run asynchronously
        self.log.debug(f'Running {len(self._delivery_jobs)} delivery jobs in the queue')
        attemps_that_never_ran = [d for d in self._delivery_jobs.values() if d.never_ran]
        self.log.debug(f"Running attempts {attemps_that_never_ran}")
        await asyncio.wait_for(
            asyncio.gather(*[create_task(d.run()) for d in attemps_that_never_ran]),
            timeout=10.0)

    def _register_attempts(self):
        finished = list()
        for idn, attempt in self._delivery_jobs.items():
            if attempt.finished:
                self.log.debug(f"Registering delivery attempt {attempt}")
                self._notification_storage.register_attempt(attempt)
                if attempt.success: self._notification_storage.set_delivered(attempt.notification)
                finished.append(idn)
        for i in finished: del self._delivery_jobs[i]

    def __call__(self):
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self.run())
        except Exception as e:
            self.log.exception("Exception", exc_info=e)
            self.log.critical(f"Scheduler died with exception {e}")
            pprint.pprint(e)
            exit(21)

    def stop(self):
        self.log.info("Stopping the scheduler main cycle")
        self._stopped.set()

    def print_notification_callable(self, ntf: Notification):
        def printme():
            self.log.debug(f"Preparing to send notification {ntf}")
            sleep(0.2)
            self.log.debug("Print me: " + pprint.pformat(ntf))
            sleep(0.2)
            self.log.debug(f"Finished to send notification {ntf}")

        return printme

    def status_summary(self):
        return {
            'jobs': len(self._delivery_jobs),
            'running': sum(1 for j in self._delivery_jobs.values() if j.running),
            'never_run': sum(1 for j in self._delivery_jobs.values() if j.never_ran),
            'finished': sum(1 for j in self._delivery_jobs.values() if j.finished),
        }


class NotificationServiceFacade(object):
    def __init__(self,
                 scheduler: NotificationDeliveryScheduler,
                 storage: NotificationStorage,
                 retention_manager: NotificationRetentionManager,
                 ):
        self._storage = storage
        self.log = logging.getLogger(self.__class__.__name__)
        self._scheduler: NotificationDeliveryScheduler = scheduler
        self._scheduler_thread = threading.Thread(target=self._scheduler, name="SchedulerT")
        self._retention_manager = retention_manager
        self._retention_manager_thread = threading.Thread(target=self._retention_manager, name="RetentionMgmt")
        self._http_thread = None

    def add_notification(self, notification: Notification):
        self.log.debug(f"Adding notification {notification} to storage")
        self._storage.add_notification(notification)

    def get_notifications_for(self, recipient: str):
        pass

    def start_sheduler(self):
        self.log.info("Starting scheduler thread")
        self._scheduler_thread.start()
        self.log.info("Scheduler thread started")

    def stop_scheduler(self):
        self.log.info("Stopping the scheduler thread ")
        self._scheduler.stop()
        self.log.info("Waiting for the scheduler thread to finish the thread")
        self._scheduler_thread.join()

    def start_retention_manager(self):
        self.log.info("Starting retention manager thread")
        self._retention_manager_thread.start()
        self.log.info("Retention manager thread started")

    def stop_retention_manager(self):
        self.log.info("Stopping the retention manager thread")
        self._retention_manager.stop()
        self.log.info("Waiting for the retention manager thread to finish")
        self._retention_manager_thread.join()
        self.log.info("Retention manager thread finished")


class HTTPRunner:

    def __init__(self, http_app: Callable):
        self._http_app = http_app
        self.log = logging.getLogger(self.__class__.__name__)
        self._uv_server = None

    def start_http(self):
        self.log.info("Starting the HTTP thread")
        self._http_thread = threading.Thread(target=self._run_uvicorn, name="HTTP-Thread")
        self._http_thread.start()
        self.log.info("HTTP thread started")

    def stop_http(self):
        self.log.info("Stopping the HTTP thread ")
        self._uv_server.handle_exit(None, None)
        self.log.info("Waiting for the HTTP thread to finish")
        self._http_thread.join()

    def _run_uvicorn(self):
        self.log.info("Starting a new event loop in current thread")
        loop = uvloop.new_event_loop()
        #loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.log.info("Running the uvicorn")
        from uvicorn import Server, Config
        uvconfig = Config(self._http_app, port=5000, log_level="debug", loop="asyncio")
        self._uv_server = Server(uvconfig)
        self._uv_server.run()


class StatsCollector:
    def __init__(self,
                 storage: NotificationStorage,
                 scheduler: NotificationDeliveryScheduler,
                 delivery_action_factory: DeliveryActionFactory
                 ):
        self._delivery_action_factory = delivery_action_factory
        self._scheduler = scheduler
        self._storage = storage
        self.counter = 0

    def get_stats(self):
        self.counter += 1
        return {
            'timestamp': Now().strftime(DATETIME_FORMAT),
            'counter': self.counter,
            'storage': self._storage.status_summary(),
            'scheduler': self._scheduler.status_summary(),
            'delivery': self._delivery_action_factory.status_summary(),
            'memory': self._get_memory_usage()
        }

    def _get_memory_usage(self):
        return {
            'resident': "{:0.02f}MB".format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
        }


class ReportStreamer:
    def __init__(self, interval: timedelta, collector: StatsCollector, sink, source):
        self._source = source
        self._interval = interval
        self._sink = sink
        self._collector = collector
        self.stopped = threading.Event()
        self.thread = None

    async def run(self):
        await self._send_headers()
        while not self.stopped.is_set():
            await self._stream_bit(self._collector.get_stats())
            try:
                msg = await asyncio.wait_for(self._source(), timeout=self._interval.total_seconds())
            except asyncio.TimeoutError:
                pass
            else:
                if msg['type'] == "http.disconnect": self.stopped.set()
        await self._finish_stream()

    async def _send_headers(self):
        await self._sink({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain'],
            ]
        })

    async def _stream_bit(self, report):
        response = pprint.pformat(report)
        await self._sink({
            'type': 'http.response.body',
            'body': f"{response}\n\n".encode('utf-8'),
            'more_body': True
        })

    async def _finish_stream(self):
        await self._sink({'type': 'http.response.body', 'body': b''})

    async def start(self):
        await self.run()


class UvicornHttpWiring(object):
    def __init__(self, facade: NotificationServiceFacade, statreporter: StatsCollector):
        self._facade = facade
        self._statreporter = statreporter

    async def __call__(self, scope, receive, send):
        assert scope['type'] == 'http'
        path = scope['path']
        if path == '/message':
            await self._add_message(scope, receive, send)
        elif path == '/status':
            await self._status(scope, send)
        elif path == '/status/stream':
            await self._steam_status(receive, send)
        else:
            await self._respond404(send)

    async def _add_message(self, scope, recieve, send):

        # data = urllib.parse.parse_qs(scope['query_string'])
        # recipient = bytes.decode(data[b'recipient'][0])
        # message = bytes.decode(data[b'message'][0])

        data = await self._get_json(recieve)
        recipient = data['recipient']
        message = data['message']

        notification = Notification(recipient, message)
        self._facade.add_notification(notification)

        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain'],
            ]
        })

        await send({
            'type': 'http.response.body',
            'body': f"{notification}".encode('utf-8'),
        })

    async def _status(self, scope, send):
        body = pprint.pformat(self._statreporter.get_stats(), indent=4, width=120)
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain'],
            ]
        })
        await send({
            'type': 'http.response.body',
            'body': body.encode('utf-8'),
        })

    async def _respond404(self, send):
        await send({
            'type': 'http.response.start',
            'status': 404,
            'headers': [
                [b'content-type', b'text/plain'],
            ]
        })
        await send({
            'type': 'http.response.body',
            'body': "Not Found".encode('utf-8'),
        })

    async def _steam_status(self, recieve, send):
        streamer = ReportStreamer(timedelta(seconds=2), self._statreporter, send, recieve)
        await streamer.start()

    async def _get_json(self, receive):
        """
        Read and return the entire body from an incoming ASGI message.
        """
        body = b''
        more_body = True

        while more_body:
            message = await receive()
            body += message.get('body', b'')
            more_body = message.get('more_body', False)

        return json.loads(body)


@dataclass
class ServiceConfig:
    scheduler_config: SchedulerConfig
    retention_config: RetentionConfig
    http_post_config: HttpPostDeliveryConfig


class NotificationServiceBuilder:
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.debug(f"Building application with config = {self.config}")

    @cached_property
    def uvicorn_http_app(self) -> UvicornHttpWiring:
        return UvicornHttpWiring(
            self.notification_service,
            StatsCollector(
                self.storage,
                self.scheduler,
                self.delivery_factory
            )
        )

    @cached_property
    def notification_service(self):
        return NotificationServiceFacade(
            scheduler=self.scheduler,
            storage=self.storage,
            retention_manager=self.retention_manager
        )

    @cached_property
    def retention_manager(self) -> NotificationRetentionManager:
        return NotificationRetentionManager(self.config.retention_config, self.storage)

    @cached_property
    def scheduler(self):
        return NotificationDeliveryScheduler(self.config.scheduler_config, self.storage, self.delivery_factory)

    @cached_property
    def storage(self): return NotificationStorage()

    @cached_property
    def delivery_factory(self) -> DeliveryActionFactory:
        return HttpPostDeliveryMethod(self.config.http_post_config)

    @cached_property
    def http_runner(self) -> HTTPRunner:
        return HTTPRunner(self.uvicorn_http_app)


def configure_root_logger():
    formatter = logging.Formatter('%(asctime)s %(levelname)-10s %(threadName)-16s %(name)-24s %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    stdrr = logging.StreamHandler()
    stdrr.setLevel(logging.INFO)
    stdrr.setFormatter(formatter)
    fh = logging.FileHandler(f'notification-service.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(stdrr)
    return logger


if __name__ == "__main__":
    logger = configure_root_logger()

    config = ServiceConfig(
        scheduler_config=SchedulerConfig(
            fail_delivery_after=timedelta(hours=1),
            retry_interval=timedelta(milliseconds=500),
            active_delivery_queue_size=100
        ),
        retention_config=RetentionConfig(
            check_interval=timedelta(seconds=30),
            archive_notifications_after=timedelta(minutes=10)
        ),
        http_post_config=HttpPostDeliveryConfig(
            url="http://localhost:5555",
            timeout=timedelta(milliseconds=500))
    )

    service_builder = NotificationServiceBuilder(config)

    service_builder.notification_service.start_sheduler()
    service_builder.notification_service.start_retention_manager()
    service_builder.notification_service.add_notification(Notification(recipient="max@kosyakov.net", message="Service Started"))
    service_builder.http_runner.start_http()

    service_stopped = threading.Event()


    def on_signal(signal, frame):
        logger.info(f"Got signal {signal}")
        service_stopped.set()
        logger.info(f"service stopped = {service_stopped}")


    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGHUP, on_signal)

    logger.info("Waiting for the signal to stop")
    while not service_stopped.is_set():
        sleep(1.0)

    logger.info("Stopping the service")
    service_builder.http_runner.stop_http()
    service_builder.notification_service.stop_scheduler()
    service_builder.notification_service.stop_retention_manager()
