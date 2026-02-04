from yt_framework.core.pipeline import DebugContext
from yt_framework.core.stage import BaseStage
from yt_framework.utils.logging import log_header


class YqlExamplesStage(BaseStage):
    def run(self, debug: DebugContext) -> DebugContext:
        self.join_tables()
        self.filter_table()
        self.select_columns()
        self.group_by_aggregate()
        self.union_tables()
        self.distinct()
        self.sort_table()
        self.limit_table()

        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("All YQL operations completed successfully!")
        self.logger.info("=" * 60)

        return debug

    def join_tables(self):
        log_header(self.logger, "YQL", "1. JOIN TABLES")

        # Preview YQL query before execution
        query = self.deps.yt_client.join_tables(
            left_table=self.config.client.orders_table,
            right_table=self.config.client.users_table,
            output_table=self.config.client.output.joined,
            on="user_id",
            how="left",
            select_columns=[
                "a.order_id",
                "a.user_id",
                "a.product",
                "a.amount",
                "b.name",
                "b.age",
                "b.city",
            ],
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        # Simple join on single column
        self.deps.yt_client.join_tables(
            left_table=self.config.client.orders_table,
            right_table=self.config.client.users_table,
            output_table=self.config.client.output.joined,
            on="user_id",
            how="left",
            select_columns=[
                "a.order_id",
                "a.user_id",
                "a.product",
                "a.amount",
                "b.name",
                "b.age",
                "b.city",
            ],
        )
        self.logger.info(f"Left join result: {self.config.client.output.joined}")

    def filter_table(self):
        log_header(self.logger, "YQL", "2. FILTER TABLE")

        # Preview YQL query before execution
        query = self.deps.yt_client.filter_table(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.filtered,
            condition="amount > 100",
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.filter_table(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.filtered,
            condition="amount > 100",
        )
        row_count = self.deps.yt_client.row_count(self.config.client.output.filtered)
        self.logger.info(f"Filtered orders (amount > 100): {row_count} rows")

    def select_columns(self):
        log_header(self.logger, "YQL", "3. SELECT COLUMNS")

        # Preview YQL query before execution
        query = self.deps.yt_client.select_columns(
            input_table=self.config.client.users_table,
            output_table=self.config.client.output.selected,
            columns=["user_id", "name"],
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.select_columns(
            input_table=self.config.client.users_table,
            output_table=self.config.client.output.selected,
            columns=["user_id", "name"],
        )
        self.logger.info("Selected columns: user_id, name")

    def group_by_aggregate(self):
        log_header(self.logger, "YQL", "4. GROUP BY AGGREGATE")

        # Preview YQL query before execution
        query = self.deps.yt_client.group_by_aggregate(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.aggregated,
            group_by="user_id",
            aggregations={
                "order_count": "count",
                "total_amount": "sum",
            },
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.group_by_aggregate(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.aggregated,
            group_by="user_id",
            aggregations={
                "order_count": "count",
                "total_amount": "sum",
            },
        )
        self.logger.info("Aggregated orders by user_id")

        # Read and display results
        results = list(
            self.deps.yt_client.read_table(self.config.client.output.aggregated)
        )
        for row in results:
            self.logger.info(
                f"  User {row['user_id']}: {row['order_count']} orders, total: {row['total_amount']}"
            )

    def union_tables(self):
        log_header(self.logger, "YQL", "5. UNION TABLES")

        # Preview YQL query before execution
        query = self.deps.yt_client.union_tables(
            tables=[
                self.config.client.orders_table,
                self.config.client.archive_orders_table,
            ],
            output_table=self.config.client.output.united,
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.union_tables(
            tables=[
                self.config.client.orders_table,
                self.config.client.archive_orders_table,
            ],
            output_table=self.config.client.output.united,
        )
        row_count = self.deps.yt_client.row_count(self.config.client.output.united)
        self.logger.info(f"United tables: {row_count} total rows")

    def distinct(self):
        log_header(self.logger, "YQL", "6. DISTINCT")

        # Preview YQL query before execution
        query = self.deps.yt_client.distinct(
            input_table=self.config.client.users_table,
            output_table=self.config.client.output.distinct,
            columns=["city"],
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.distinct(
            input_table=self.config.client.users_table,
            output_table=self.config.client.output.distinct,
            columns=["city"],
        )
        cities = list(
            self.deps.yt_client.read_table(self.config.client.output.distinct)
        )
        self.logger.info(f"Distinct cities: {[c['city'] for c in cities]}")

    def sort_table(self):
        log_header(self.logger, "YQL", "7. SORT TABLE")

        # Preview YQL query before execution
        query = self.deps.yt_client.sort_table(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.sorted,
            order_by="amount",
            ascending=False,
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.sort_table(
            input_table=self.config.client.orders_table,
            output_table=self.config.client.output.sorted,
            order_by="amount",
            ascending=False,
        )
        self.logger.info("Sorted orders by amount (descending)")

    def limit_table(self):
        log_header(self.logger, "YQL", "8. LIMIT TABLE")

        # Preview YQL query before execution
        query = self.deps.yt_client.limit_table(
            input_table=self.config.client.output.sorted,
            output_table=self.config.client.output.limited,
            limit=3,
            dry_run=True,
        )
        self.logger.info(f"YQL preview (dry run):\n{query}")

        self.deps.yt_client.limit_table(
            input_table=self.config.client.output.sorted,
            output_table=self.config.client.output.limited,
            limit=3,
        )
        top3 = list(self.deps.yt_client.read_table(self.config.client.output.limited))
        self.logger.info("Top 3 orders by amount:")
        for order in top3:
            self.logger.info(
                f"  Order {order['order_id']}: {order['product']} - {order['amount']}"
            )
