from typing import List

def filter_recent_tables(tables: List[str], year: int, start_month: int) -> List[str]:
    """Filter tables by year and month suffix."""
    filtered = []
    for t in tables:
        parts = t.split('_')
        if len(parts) > 2 and parts[-2].isdigit() and parts[-1].isdigit():
            y, m = int(parts[-2]), int(parts[-1])
            if y > year or (y == year and m >= start_month):
                filtered.append(t)
    return filtered

def get_matching_tag_ids(tags_df, patterns):
    """Extract tag IDs whose tagpath matches any pattern."""
    condition = None
    for pattern in patterns:
        cond = tags_df.tagpath.rlike(pattern)
        condition = cond if condition is None else condition | cond
    return tags_df.filter(condition).select("id").rdd.flatMap(lambda x: x).collect()
