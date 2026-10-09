import { fileURLToPath } from 'node:url';

export const rootURL = new URL('../..', import.meta.url);
export const root = fileURLToPath(rootURL);
