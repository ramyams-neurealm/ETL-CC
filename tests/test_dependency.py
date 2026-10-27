from etl_cc.engines import calculate_migration_waves

def test_waves():
    assert calculate_migration_waves([("A", "C"), ("B", "C")]) == [["A", "B"], ["C"]]
