import json
import typer
from typing import Type, Annotated, Optional
from pydantic import BaseModel
from pydantic.json_schema import (
    GenerateJsonSchema,
    JsonSchemaMode,
    JsonSchemaValue,
)
from pydantic_core import CoreSchema

from lib import console


def _immaterial_model_patch(json_schema: JsonSchemaValue):
    json_schema["$id"] = json_schema.pop("title")

    for property in json_schema.get("properties", {}).values():
        for key in ["allOf", "anyOf"]:
            if key in property:
                property["oneOf"] = property.pop(key)


class ImmaterialJsonSchemaGenerator(GenerateJsonSchema):
    def __init__(self, by_alias: bool = True, ref_template=...):
        super().__init__(by_alias, "{model}")

    def generate(
        self, schema: CoreSchema, mode: JsonSchemaMode = "validation"
    ) -> JsonSchemaValue:
        json_schema = super().generate(schema, mode)

        _immaterial_model_patch(json_schema)

        if "$defs" in json_schema:
            definitions = json_schema.pop("$defs")
            for model in definitions.values():
                _immaterial_model_patch(model)

            json_schema["definitions"] = definitions

        return json_schema

    def field_title_should_be_set(self, schema) -> bool:
        return False


def config_schema_for_class(config: Type[BaseModel]):
    def config_schema(
        output: Annotated[
            Optional[typer.FileTextWrite],
            typer.Option(
                "--output",
                "-o",
                help="Output JSON file. If omitted, will print to stdout",
            ),
        ] = None,
        docs: Annotated[
            bool,
            typer.Option("--docs", help="Ensure compatibility with sphinx-immaterial"),
        ] = False,
    ):
        schema = config.model_json_schema(
            schema_generator=ImmaterialJsonSchemaGenerator
            if docs
            else GenerateJsonSchema
        )

        if output:
            json.dump(schema, output, indent=4)
        else:
            console.print_json(data=schema, indent=4)

    return config_schema
