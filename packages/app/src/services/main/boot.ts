/* tslint:disable:no-import-side-effect */

/*
** This file purpose is to handle startup side effects in main process
*/
import { app, ipcMain } from 'electron';

import { startSessionsListening } from '../api/sessions';

export default () => {
  
  // Initialize our in-repo IPC handler (replaces stream-electron-ipc side-effect)
  require('../lib/firstConnectionHandler');

  startSessionsListening();

  ipcMain.on('get-is-packaged', (event) => {
    event.returnValue = app.isPackaged;
  });
  
};
