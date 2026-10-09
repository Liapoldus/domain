// JSON boundaries return unknown; each fixture consumer supplies its wire shape.
export function parseJson(source: string): unknown {
  const value: unknown = JSON.parse(source);
  return value;
}

export type Field = { name: string; type: string; primaryKey?: boolean; required?: boolean; unique?: boolean; references?: { entity: string; field: string } };
export type Model = { schemaVersion: string; entities: { name: string; ownerGroup: string; fields: Field[] }[]; migrations?: unknown[] };

export type Schema = {
  $schema?: string;
  type?: string | string[];
  properties?: Record<string, Schema>;
  $defs?: Record<string, Schema>;
  required?: string[];
  enum?: string[];
  additionalProperties?: boolean | Schema;
};

export function propertiesOf(schema: Schema): Record<string, Schema> {
  if (!schema.properties) throw new Error('schema must declare properties');
  return schema.properties;
}

export function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('required contract value is missing');
  return value;
}
