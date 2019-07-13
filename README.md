# Async Python Experminental Architectures


The purpose of the project is to test the architectures based on async Python  features based on the model of notification service, which does the following:
* recevied a message from either HTTP or other transport layer
* send the message to HTTP or other transport layer (based on user preferences)

Message delivery is attempted multiple time unit either success or timeout.


## Use of Uvicorn

In order to make uvicorn run in a seprate thread with a custom pre-confugred event loop, the following modifications to the uvicorn are required:

* comment out event loop setup 
* comment out signal handler registration (works only in the Main thread, and we want to handle it ourselves)

```patch
--- env/lib/pythonX.Y/site-packages/uvicorn/main.py
+++ env/lib/pythonX.Y/site-packages/uvicorn/main.py
@@ -302,7 +302,7 @@
         self.last_notified = 0
 
     def run(self, sockets=None, shutdown_servers=True):
-        self.config.setup_event_loop()
+        #self.config.setup_event_loop()
         loop = asyncio.get_event_loop()
         loop.run_until_complete(self.serve(sockets=sockets))
 
@@ -316,7 +316,7 @@
         self.logger = config.logger_instance
         self.lifespan = config.lifespan_class(config)
 
-        self.install_signal_handlers()
+#        self.install_signal_handlers()
 
         self.logger.info("Started server process [{}]".format(process_id))
         await self.startup(sockets=sockets)
```

