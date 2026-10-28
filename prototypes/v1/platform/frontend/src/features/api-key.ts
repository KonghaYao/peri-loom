// The key name predates the rename to "API Key". It stays so an upgrade keeps
// the key already stored in this browser.
const API_KEY_STORAGE = 'peri-loom:v1:console:master-key';
const REMEMBER_STORAGE = 'peri-loom:v1:console:remember-api-key';

export function loadRememberedAPIKey(storage: Storage): string {
  return storage.getItem(API_KEY_STORAGE) ?? '';
}

export function saveRememberedAPIKey(storage: Storage, value: string): void {
  if (!value.trim()) throw new Error('API Key must not be empty');
  storage.setItem(API_KEY_STORAGE, value);
}

export function clearRememberedAPIKey(storage: Storage): void {
  storage.removeItem(API_KEY_STORAGE);
}

export function loadRememberPreference(storage: Storage): boolean {
  return storage.getItem(REMEMBER_STORAGE) !== '0';
}

export function saveRememberPreference(storage: Storage, remember: boolean): void {
  storage.setItem(REMEMBER_STORAGE, remember ? '1' : '0');
}

// Unchecking removes the stored key instead of merely skipping this save: the
// user asked this browser to forget it, and a stale key would auto-connect.
export function syncRememberedAPIKey(storage: Storage, remember: boolean, value: string): boolean {
  if (remember) {
    saveRememberedAPIKey(storage, value);
    return true;
  }
  clearRememberedAPIKey(storage);
  return false;
}
