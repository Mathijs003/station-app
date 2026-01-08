import { Observable } from 'rxjs';
import { firstConnectionHandler } from '../lib/firstConnectionHandler';
import rpcchannel from 'stream-json-rpc';
import { isPackaged } from '../../utils/env';
import { servicesDuplexNamespace } from '../api/const';
import { ServicePeerHandler } from '../lib/class';

export const observeNewClients = () => {
  return new Observable(observer => {
    firstConnectionHandler(duplex => {
      const channel = rpcchannel(duplex, {
        forwardErrors: true, // !isPackaged,
      });
      observer.next(new ServicePeerHandler(channel, !isPackaged));
    }, servicesDuplexNamespace);
  });
};
