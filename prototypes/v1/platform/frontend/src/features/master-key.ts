const STORAGE_KEY = 'peri-loom:v1:console:master-key';

export function loadRememberedMasterKey(storage: Storage): string {
  return storage.getItem(STORAGE_KEY) ?? '';
}

export function saveRememberedMasterKey(storage: Storage, value: string): void {
  if (!value.trim()) throw new Error('Master API key must not be empty');
  storage.setItem(STORAGE_KEY, value);
}

export function clearRememberedMasterKey(storage: Storage): void {
  storage.removeItem(STORAGE_KEY);
}
