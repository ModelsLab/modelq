from tasks import modelq_app
import time

if __name__ == "__main__":
    modelq_app.start_workers()

    # Keep the worker running indefinitely
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")