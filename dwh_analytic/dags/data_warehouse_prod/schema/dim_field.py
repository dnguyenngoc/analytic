class DimFieldModel:
    def __init__(
        self,
        *kwargs,
        field_key: str,
        project_id: str,
        name: str,
        control_type: str,
        default_value: str = None,
        counted_character: bool,
        counted_character_date_from_key: int,
        counted_character_time_from_key: int,
        counted_character_date_to_key: int,
        counted_character_time_to_key: int,
        counted_character_from_timestamp: str,
        counted_character_to_timestamp: str,
        is_sub_field: bool = False,

    ):
        self.field_key = field_key
        self.project_id = project_id
        self.name = name
        self.control_type = control_type
        self.default_value = default_value
        self.counted_character = counted_character
        self.counted_character_date_from_key = counted_character_date_from_key
        self.counted_character_time_from_key = counted_character_time_from_key
        self.counted_character_date_to_key = counted_character_date_to_key
        self.counted_character_time_to_key = counted_character_time_to_key
        self.counted_character_from_timestamp = counted_character_from_timestamp
        self.counted_character_to_timestamp = counted_character_to_timestamp
        self.is_sub_field = is_sub_field