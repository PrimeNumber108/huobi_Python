import threading
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from huobi.connection.impl.private_def import *
from huobi.utils.time_service import get_current_timestamp


def watch_dog_job(*args):
    """Kiểm tra các kết nối và quản lý trạng thái WebSocket."""
    watch_dog_obj = args[0]

    for idx, websocket_manage in enumerate(watch_dog_obj.websocket_manage_list):
        if websocket_manage.request.auto_close:
            continue  # Bỏ qua nếu auto_close được bật

        ts = get_current_timestamp() - websocket_manage.last_receive_time
        if websocket_manage.state == ConnectionState.CONNECTED:
            if watch_dog_obj.is_auto_connect and ts > watch_dog_obj.heart_beat_limit_ms:
                watch_dog_obj.logger.warning(f"[Sub][{websocket_manage.id}] No response from server")
                websocket_manage.close_and_wait_reconnect(watch_dog_obj.wait_reconnect_millisecond())

        elif websocket_manage.state == ConnectionState.WAIT_RECONNECT:
            watch_dog_obj.logger.warning("[Sub] call re_connect")
            websocket_manage.re_connect()

        elif websocket_manage.state == ConnectionState.CLOSED_ON_ERROR:
            if watch_dog_obj.is_auto_connect:
                websocket_manage.close_and_wait_reconnect(watch_dog_obj.reconnect_after_ms)


class WebSocketWatchDog(threading.Thread):
    """Quản lý WebSocket và duy trì kết nối."""
    mutex = threading.Lock()
    websocket_manage_list = []

    def __init__(self, is_auto_connect=True, heart_beat_limit_ms=30000, reconnect_after_ms=60000):
        threading.Thread.__init__(self)
        self.is_auto_connect = is_auto_connect
        self.heart_beat_limit_ms = heart_beat_limit_ms
        self.reconnect_after_ms = max(reconnect_after_ms, heart_beat_limit_ms)

        # Khởi tạo logger
        self.logger = self._setup_logger()

        # Khởi tạo scheduler với ThreadPoolExecutor
        executors = {'default': ThreadPoolExecutor(max_workers=50)}
        self.scheduler = BackgroundScheduler(executors=executors)
        self.scheduler.add_job(watch_dog_job, "interval", max_instances=50, seconds=30, args=[self])
        self.start()

    def _setup_logger(self):
        """Thiết lập logger."""
        logger = logging.getLogger("huobi-client")
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def run(self):
        """Chạy scheduler trong thread."""
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def stop(self):
        """Dừng scheduler và thread an toàn."""
        self.logger.info("Stopping WebSocketWatchDog...")
        self.scheduler.shutdown(wait=False)  # Dừng scheduler ngay lập tức
        self.join()  # Chờ thread kết thúc

    def on_connection_created(self, websocket_manage):
        """Thêm kết nối WebSocket mới."""
        with self.mutex:
            self.websocket_manage_list.append(websocket_manage)

    def on_connection_closed(self, websocket_manage):
        """Xóa kết nối WebSocket đã đóng."""
        with self.mutex:
            self.websocket_manage_list.remove(websocket_manage)

    def wait_reconnect_millisecond(self):
        """Tính thời gian chờ trước khi thử lại kết nối."""
        wait_millisecond = max(self.reconnect_after_ms - self.heart_beat_limit_ms, 1000)
        now_ms = get_current_timestamp()
        return wait_millisecond + now_ms
