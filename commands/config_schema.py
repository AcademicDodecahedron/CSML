import json
import typer
from typing import Type, Annotated, Optional, cast
from pydantic import BaseModel, PydanticUserError
from pydantic.json_schema import (
    GenerateJsonSchema,
    JsonSchemaMode,
    JsonSchemaValue,
    JsonRef,
    _sort_json_schema,
    _get_all_json_refs,
)
from pydantic_core import CoreSchema

from lib import console


class ParentRef(BaseModel):
    ref: JsonRef
    level: int


class GenerateJsonSchemaNestedDefinitions(GenerateJsonSchema):
    def generate(
        self, schema: CoreSchema, mode: JsonSchemaMode = "validation"
    ) -> JsonSchemaValue:
        self._mode = mode
        if self._used:
            raise PydanticUserError(
                "This JSON schema generator has already been used to generate a JSON schema. "
                f"You must create a new instance of {type(self).__name__} to generate a new JSON schema.",
                code="json-schema-already-used",
            )

        json_schema: JsonSchemaValue = self.generate_inner(schema)
        json_ref_counts = self.get_json_ref_counts(json_schema)

        # Remove the top-level $ref if present; note that the _generate method already ensures there are no sibling keys
        ref = cast(JsonRef, json_schema.get("$ref"))
        while ref is not None:  # may need to unpack multiple levels
            ref_json_schema = self.get_schema_from_definitions(ref)
            if json_ref_counts[ref] > 1 or ref_json_schema is None:
                # Keep the ref, but use an allOf to remove the top level $ref
                json_schema = {"allOf": [{"$ref": ref}]}
            else:
                # "Unpack" the ref since this is the only reference
                json_schema = (
                    ref_json_schema.copy()
                )  # copy to prevent recursive dict reference
                json_ref_counts[ref] -= 1
            ref = cast(JsonRef, json_schema.get("$ref"))

        self._garbage_collect_definitions(json_schema)
        self._embed_definitions(json_schema)

        definitions_remapping = self._build_definitions_remapping()
        json_schema = definitions_remapping.remap_json_schema(json_schema)

        self._used = True
        return _sort_json_schema(json_schema)

    def _embed_definitions(self, schema: JsonSchemaValue):
        refs = _get_all_json_refs(schema)

        if len(refs) > 0:
            defs = {}

            for ref in refs:
                def_ref = self.json_to_defs_refs[ref]
                child = self.definitions[def_ref]

                self._embed_definitions(child)
                child["$$target"] = ref
                defs[def_ref] = child

            schema["$defs"] = defs


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
        nested_defs: Annotated[
            bool,
            typer.Option(
                "--nested-defs",
                help="Whether to use nested structure for $defs instead of flat (more readable when using sphinx-jsonschema)",
            ),
        ] = False,
    ):
        schema = config.model_json_schema(
            schema_generator=GenerateJsonSchemaNestedDefinitions
            if nested_defs
            else GenerateJsonSchema,
        )

        if output:
            json.dump(schema, output, indent=4)
        else:
            console.print_json(data=schema, indent=4)

    return config_schema
