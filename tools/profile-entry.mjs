import path from 'node:path';
import {pathToFileURL} from 'node:url';

// CPU profiles flush on normal exit; owned services otherwise end on a signal.
process.on('SIGINT', () => process.exit(0));
process.argv.splice(1, 1);
await import(pathToFileURL(path.resolve(process.argv[1])).href);
