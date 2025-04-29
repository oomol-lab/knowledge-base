import React from "react";
import styles from "./Navigator.module.less";

import { Tabs } from "antd";
import { useLocation, useNavigate } from "react-router-dom";
import { context } from "./StoreContext";
import { useVal } from "use-value-enhancer";

export const Navigator: React.FC<{}> = () => {
  const navigate = useNavigate();
  const pathname = useLocation().pathname;
  const activeKey = pathname.split("/")[1] || undefined;
  const store = React.useContext(context).queryStore;
  const lastQueryText = useVal(store.$.lastQueryText);
  const onChange = React.useCallback(
    (activeKey: string) => {
      if (activeKey === "query" && lastQueryText !== null) {
        navigate(`/query?${new URLSearchParams({ query: lastQueryText })}`);
      } else {
        navigate("/" + activeKey);
      }
    },
    [navigate, lastQueryText],
  );
  return (
    <div className={styles.navigator}>
      <Tabs
        activeKey={activeKey}
        onChange={onChange}
        items={[
          {
            key: "scanner",
            label: "知识库",
          },
          {
            key: "query",
            label: "查询",
          },
        ]} />
    </div>
  );
};