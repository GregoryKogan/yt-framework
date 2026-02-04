from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage


class SetupDataStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.logger.info("Setting up sample data...")

        self.create_users_table()
        self.create_orders_table()
        self.create_archive_orders_table()

        return debug

    def create_users_table(self):
        users = [
            {"user_id": 1, "name": "Alice", "age": 30, "city": "Moscow"},
            {"user_id": 2, "name": "Bob", "age": 25, "city": "SPB"},
            {"user_id": 3, "name": "Charlie", "age": 35, "city": "Moscow"},
            {"user_id": 4, "name": "Diana", "age": 28, "city": "Kazan"},
        ]
        self.deps.yt_client.write_table(self.config.client.users_table, users)
        self.logger.info(f"Created users table: {self.config.client.users_table}")
    
    def create_orders_table(self):
        orders = [
            {"order_id": 1, "user_id": 1, "product": "Laptop", "amount": 1000},
            {"order_id": 2, "user_id": 1, "product": "Mouse", "amount": 50},
            {"order_id": 3, "user_id": 2, "product": "Keyboard", "amount": 100},
            {"order_id": 4, "user_id": 3, "product": "Monitor", "amount": 500},
            {"order_id": 5, "user_id": 3, "product": "Laptop", "amount": 1200},
        ]
        self.deps.yt_client.write_table(self.config.client.orders_table, orders)
        self.logger.info(f"Created orders table: {self.config.client.orders_table}")

    def create_archive_orders_table(self):
        archive_orders = [
            {"order_id": 100, "user_id": 1, "product": "Phone", "amount": 800},
            {"order_id": 101, "user_id": 4, "product": "Tablet", "amount": 600},
        ]
        self.deps.yt_client.write_table(self.config.client.archive_orders_table, archive_orders)
        self.logger.info(f"Created archive orders table: {self.config.client.archive_orders_table}")
