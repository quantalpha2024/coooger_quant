import datetime as dt
import time
import json
import gc
import logging
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
import requests
from clickhouse_driver import Client
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('binance_data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BinanceDataCollector:
    """币安K线数据收集器"""

    def __init__(self, clickhouse_config: Dict):
        self.client = None
        self.clickhouse_config = clickhouse_config
        self.setup_database()
        # 内存监控
        self.memory_threshold_mb = 500  # 内存阈值500MB
        self.request_timeout = 30  # 请求超时时间
        self.max_retries = 3  # 最大重试次数

    def _init_clickhouse_client(self) -> Optional[Client]:
        """初始化ClickHouse客户端，增加异常处理"""
        try:
            client = Client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                user=self.clickhouse_config['user'],
                password=self.clickhouse_config['password'],
                database=self.clickhouse_config['database'],
                connect_timeout=10,
                send_receive_timeout=300
            )
            # 测试连接
            client.execute('SELECT 1')
            logger.info("ClickHouse连接成功")
            return client
        except Exception as e:
            logger.error(f"ClickHouse连接失败: {e}")
            return None

    def _check_memory_usage(self) -> bool:
        """检查内存使用情况"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            if memory_mb > self.memory_threshold_mb:
                logger.warning(f"内存使用较高: {memory_mb:.2f}MB, 进行垃圾回收")
                gc.collect()
                return False
            return True
        except ImportError:
            # 如果没有psutil，跳过内存检查
            return True

    def setup_database(self) -> None:
        """设置数据库表结构，增加异常处理"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if self.client is None:
                    self.client = self._init_clickhouse_client()
                    if self.client is None:
                        raise Exception("数据库连接失败")

                drop_table_sql = "DROP TABLE IF EXISTS bz_kline"
                self.client.execute(drop_table_sql)
                logger.info("已删除现有表（如果存在）")

                create_table_query = '''
                CREATE TABLE IF NOT EXISTS bz_kline (
                    symbol String,
                    candle_begin_time DateTime,
                    open Float32,
                    high Float32,
                    low Float32,
                    close Float32,
                    volume Float32,
                    amount Float32,
                    candle_end_time DateTime
                ) ENGINE = MergeTree()
                ORDER BY (symbol, candle_begin_time)
                '''
                self.client.execute(create_table_query)
                logger.info("数据库表初始化完成")
                break
            except Exception as e:
                logger.error(f"数据库设置失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)  # 等待后重试
                self.client = None  # 重置连接

    @staticmethod
    def get_binance_kline(symbol: str, interval: str, start_time: dt.datetime,
                          end_time: dt.datetime, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """获取币安交易所的K线数据，增加重试机制"""
        url = "https://fapi.binance.com/fapi/v1/klines"

        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": str(int(start_time.timestamp() * 1000)),
            "endTime": str(int(end_time.timestamp() * 1000)),
            "limit": "1500"
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = json.loads(response.text)
                if not data:
                    return None

                df = pd.DataFrame(data).iloc[:, 0:8]
                df.columns = [
                    'candle_begin_time', 'open', 'high', 'low', 'close',
                    'volume', 'candle_end_time', 'amount'
                ]

                # 类型转换
                float_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
                for col in float_columns:
                    df[col] = df[col].astype("float")

                # 时间戳转换
                time_columns = ['candle_begin_time', 'candle_end_time']
                for col in time_columns:
                    df[col] = [dt.datetime.fromtimestamp(x / 1000.0) for x in df[col]]

                return df[['candle_begin_time', 'close', 'open', 'low', 'high',
                           'volume', 'candle_end_time', 'amount']]

            except requests.exceptions.RequestException as e:
                logger.warning(f"获取 {symbol} K线数据失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    logger.error(f"获取 {symbol} K线数据最终失败")
                    return None
                time.sleep(2 ** attempt)  # 指数退避
            except Exception as e:
                logger.error(f"处理 {symbol} 数据时发生未知错误: {e}")
                return None

        return None

    @staticmethod
    def get_lbank_instruments(max_retries: int = 3) -> List[Dict]:
        """获取LBank永续合约平台的所有交易币对，增加重试机制"""
        url = "https://lbkperp.lbank.com/cfd/openApi/v1/pub/instrument?productGroup=SwapU"

        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                data = response.json()
                instruments = data.get("data", [])

                if not instruments:
                    logger.warning("未找到交易对信息")
                    return []

                result = [
                    {
                        "symbol": inst.get("symbol"),
                        "baseCoin": inst.get("baseCurrency"),
                        "quoteCoin": inst.get("priceCurrency"),
                    }
                    for inst in instruments
                ]

                logger.info(f"成功获取 {len(result)} 个交易对")
                return result

            except Exception as e:
                logger.warning(f"获取LBank交易对失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    logger.error("获取LBank交易对最终失败")
                    return []
                time.sleep(2 ** attempt)  # 指数退避

        return []

    def batch_insert_data(self, data: pd.DataFrame, batch_size: int = 500) -> None:
        """分批次插入数据避免内存溢出，增加异常处理"""
        if data.empty:
            return

        total_rows = len(data)
        data = data.replace({np.nan: None})

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = data.iloc[start:end]

            try:
                insert_data = [row.tolist() for _, row in batch.iterrows()]

                self.client.execute(
                    f"INSERT INTO bz_kline ({', '.join(batch.columns)}) VALUES",
                    insert_data,
                    types_check=True
                )
                logger.info(f"已插入 {end}/{total_rows} 行数据")

                # 插入后立即释放内存
                del insert_data

            except Exception as e:
                logger.error(f"插入数据失败: {e}")
                # 可以在这里添加重试逻辑或跳过当前批次
                continue
            finally:
                # 确保释放内存
                if start % (batch_size * 5) == 0:  # 每5个批次检查一次内存
                    self._check_memory_usage()
                    gc.collect()

    def collect_data(self, start_date: dt.datetime, end_date: dt.datetime) -> None:
        """主数据收集函数，增加全面的异常处理"""
        try:
            instruments = self.get_lbank_instruments()
            if not instruments:
                logger.error("无法获取交易对信息，程序退出")
                return

            usdt_pairs = [inst for inst in instruments if inst.get('quoteCoin') == "USDT"]
            logger.info(f"找到 {len(usdt_pairs)} 个USDT交易对")

            for i, instrument in enumerate(usdt_pairs):
                symbol = instrument.get('symbol')
                if not symbol:
                    logger.warning("跳过无效的交易对（无symbol）")
                    continue

                logger.info(f"处理交易对 ({i + 1}/{len(usdt_pairs)}): {symbol}")

                current_date = start_date
                days_processed = 0
                total_days = (end_date - start_date).days + 1

                while current_date < end_date:
                    try:
                        next_date = min(current_date + dt.timedelta(days=1), end_date)

                        # 检查内存使用情况
                        if not self._check_memory_usage():
                            logger.warning("内存使用过高，暂停处理")
                            time.sleep(10)

                        df = self.get_binance_kline(
                            symbol=symbol,
                            interval='5m',
                            start_time=current_date,
                            end_time=next_date
                        )

                        if df is not None and not df.empty:
                            df['symbol'] = symbol
                            insert_cols = [
                                "symbol", "candle_begin_time", "open", "high", "low",
                                "close", "volume", "amount", "candle_end_time"
                            ]
                            self.batch_insert_data(df[insert_cols])

                            # 及时释放DataFrame内存
                            del df
                            gc.collect()

                        days_processed += 1
                        if days_processed % 10 == 0:  # 每处理10天打印一次进度
                            logger.info(f"{symbol} 进度: {days_processed}/{total_days} 天")

                        time.sleep(1)  # 避免请求过于频繁

                    except Exception as e:
                        logger.error(f"处理 {symbol} 日期 {current_date} 数据时出错: {e}")
                        # 跳过当前日期，继续处理下一个

                    finally:
                        current_date = current_date + dt.timedelta(days=1)

                logger.info(f"完成交易对 {symbol} 的数据收集")

        except Exception as e:
            logger.error(f"数据收集过程发生严重错误: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """清理资源"""
        try:
            if self.client:
                self.client.disconnect()
                logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"清理资源时发生错误: {e}")

        # 强制垃圾回收
        gc.collect()
        logger.info("内存清理完成")


def main():
    """主函数"""
    try:
        # ClickHouse配置
        clickhouse_config = {
            'host': 'cc-3nsqvaflp79lvvs06.clickhouse.ads.aliyuncs.com',
            'port': 3306,
            'user': 'ft_quant_admin',
            'password': 'z9CEdnxjTozUVv!!jH47Ln#',
            'database': 'contract_analysis'
        }

        logger.info("启动币安数据收集器")

        # 初始化数据收集器
        collector = BinanceDataCollector(clickhouse_config)

        # 设置时间范围
        start_date = dt.datetime(2024, 1, 1, 0, 0)
        end_date = dt.datetime.now().replace(second=0, microsecond=0)

        logger.info(f"数据收集范围: {start_date} 到 {end_date}")

        # 开始收集数据
        collector.collect_data(start_date, end_date)

        logger.info("数据收集任务完成")

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        logger.info("程序结束")


if __name__ == "__main__":
    main()