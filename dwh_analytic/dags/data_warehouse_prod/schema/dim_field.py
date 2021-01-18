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
        is_sub_field: bool = False,

    ):
        self.field_key = field_key
        self.project_id = project_id
        self.name = name
        self.control_type = control_type
        self.default_value = default_value
        self.counted_character = counted_character
        self.is_sub_field = is_sub_field