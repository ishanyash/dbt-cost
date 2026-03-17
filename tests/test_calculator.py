from dbt_cost.core.calculator import bytes_to_cost, format_bytes


class TestBytesToCost:
    def test_one_tb_default_price(self):
        assert bytes_to_cost(1_000_000_000_000) == 6.25

    def test_one_gb(self):
        cost = bytes_to_cost(1_000_000_000)
        assert round(cost, 6) == 0.00625

    def test_zero_bytes(self):
        assert bytes_to_cost(0) == 0.0

    def test_custom_price(self):
        assert bytes_to_cost(1_000_000_000_000, price_per_tb=5.0) == 5.0

    def test_realistic_estimate(self):
        # 22.1 GB at $6.25/TB = $0.138125
        cost = bytes_to_cost(22_100_000_000)
        assert round(cost, 3) == 0.138


class TestFormatBytes:
    def test_terabytes(self):
        assert format_bytes(1_500_000_000_000) == "1.5 TB"

    def test_gigabytes(self):
        assert format_bytes(2_400_000_000) == "2.4 GB"

    def test_megabytes(self):
        assert format_bytes(150_000_000) == "150.0 MB"

    def test_kilobytes(self):
        assert format_bytes(5_000) == "5.0 KB"

    def test_zero(self):
        assert format_bytes(0) == "0 B"

    def test_boundary_tb(self):
        assert format_bytes(1_000_000_000_000) == "1.0 TB"

    def test_boundary_gb(self):
        assert format_bytes(1_000_000_000) == "1.0 GB"
