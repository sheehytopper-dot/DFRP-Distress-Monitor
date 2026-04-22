from parsers.amounts import find_all_amounts, largest_amount


def test_single_dollar_amount():
    assert find_all_amounts("Bid $1,234.56") == [1234]


def test_multiple_amounts_sorted_max():
    text = "Min bid $50,000. Adjudged value $2,450,000.00. Taxes $12,345."
    assert largest_amount(text) == 2_450_000


def test_plain_digits_need_4_plus_to_match():
    # Exclude short numbers like cause numbers "Cause 123" but keep "$999" via $ form
    assert find_all_amounts("Cause 123, amount $999") == []  # $999 = 3 digits, no comma, below threshold
    assert find_all_amounts("Cause 2021-42, amount $1000") == [1000]


def test_no_amounts():
    assert largest_amount("no dollars here") is None
    assert find_all_amounts("") == []


def test_dollar_with_space():
    assert find_all_amounts("$ 1,500,000") == [1_500_000]


def test_skips_years_without_dollar_sign():
    assert find_all_amounts("filed in 2024 for account 2021") == []
