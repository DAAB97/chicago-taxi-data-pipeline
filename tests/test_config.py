from src.download_to_bronze import FIELDS
def test_required_fields(): assert {"trip_id","trip_start_timestamp","trip_total"}.issubset(FIELDS)
