import logging

logger = logging.getLogger('my_logger_cn')
logger.setLevel(logging.DEBUG)  # 设置最低的日志级别
# 创建一个文件处理器，用于写入日志文件
file_handler = logging.FileHandler('my_log_file.log')
file_handler.setLevel(logging.DEBUG)

# 创建一个格式化器并设置给处理器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 添加处理器到logger
logger.addHandler(file_handler)