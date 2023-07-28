from datetime import datetime


def handle_path_date(date: str = "today"):
    """
    Handle date based on given date string.

    Parameters:
    date (str): The date to be processed. Can be 'today' to get the current date, 'all' to return a wildcard date, or in the format 'YYYY-MM-DD'.

    Returns:
    str: A string in the format "year=YYYY/month=MM/day=DD" or "year=*/month=*/day=*"
    """

    if date == "all":
        return "year=*/month=*/day=*"
    elif date.lower() == "today":
        return datetime.now().strftime("year=%Y/month=%m/day=%d")
    else:
        year, month, day = map(int, date.split("-"))
        return f"year={year}/month={month:02d}/day={day:02d}"
