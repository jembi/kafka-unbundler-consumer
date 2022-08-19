import { Bundle, ResourceMap } from './types';

export function splitResources(bundle: Bundle) {
  const resourceMap: ResourceMap = {};

  bundle.entry.forEach(entry => {
    if (!resourceMap[entry.resource.resourceType]) {
      resourceMap[entry.resource.resourceType] = [];
    }
    resourceMap[entry.resource.resourceType].push({
      key: entry.resource.id,
      value: JSON.stringify(entry),
    });
  });

  return resourceMap;
}
