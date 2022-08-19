export interface Bundle {
  entry: Entry[];
}

export interface ResourceMap {
  [name: string]: MapItem[];
}

interface MapItem {
  key: string;
  value: string;
}

interface Entry {
  resource: Resource;
}

interface Resource {
  resourceType: string;
  id: string;
}
