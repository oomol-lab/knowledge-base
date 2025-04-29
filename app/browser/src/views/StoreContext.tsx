import React from "react";

import { val } from "value-enhancer";
import { useVal } from "use-value-enhancer";
import { message } from "antd";
import { QueryStore, ScannerStore } from "../store";

export type StoreContext = {
  readonly scannerStore: ScannerStore | null;
  readonly queryStore: QueryStore;
};

export const context = React.createContext<StoreContext>(null as any);

export const StoreContextProvider: React.FC<React.PropsWithChildren<{}>> = ({ children }) => {
  const [value$, loadFailure$] = React.useMemo(
    () => {
      const loadFailure$ = val(false);
      const value$ = val<StoreContext>({
        scannerStore: null,
        queryStore: new QueryStore(),
      });
      ScannerStore.load()
        .then((scannerStore) => {
          value$.set({
            ...value$.value,
            scannerStore,
          });
        })
        .catch(error => {
          console.error(error);
          message.error(error.message);
          loadFailure$.set(true);
        });
      return [value$, loadFailure$];
    },
    [],
  );
  const value = useVal(value$);
  const lastLoadFailure = useVal(loadFailure$);

  if (lastLoadFailure) {
    return null;
  }
  return (
    <context.Provider value={value}>
      {children}
    </context.Provider>
  );
}