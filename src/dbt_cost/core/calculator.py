DEFAULT_BQ_PRICE_PER_TB = 6.25  # USD, on-demand pricing (1 TB = 10^12 bytes)


def bytes_to_cost(total_bytes: int, price_per_tb: float = DEFAULT_BQ_PRICE_PER_TB) -> float:
    """Convert bytes processed to estimated cost in USD."""
    tb = total_bytes / 1_000_000_000_000  # 10^12, decimal TB matching BQ billing
    return tb * price_per_tb


def format_bytes(total_bytes: int) -> str:
    """Format bytes to human-readable string (decimal units, matching BQ console)."""
    if total_bytes >= 1_000_000_000_000:
        return f"{total_bytes / 1_000_000_000_000:.1f} TB"
    if total_bytes >= 1_000_000_000:
        return f"{total_bytes / 1_000_000_000:.1f} GB"
    if total_bytes >= 1_000_000:
        return f"{total_bytes / 1_000_000:.1f} MB"
    if total_bytes == 0:
        return "0 B"
    return f"{total_bytes / 1_000:.1f} KB"
