import React from "react";
import styles from "./ScannerPage.module.less";

import { Skeleton, Result, Steps, List, Button, Divider, Progress, Typography } from "antd";
import { ScanOutlined, ProfileTwoTone, SyncOutlined, FilePdfTwoTone, PauseOutlined } from "@ant-design/icons";
import { useVal } from "use-value-enhancer";
import { ScanningStore, ScanningPhase, FileOperation } from "../store";
import { Sources } from "./Sources";
import { context } from "./StoreContext";

const { Title, Paragraph } = Typography;

export const ScannerPage: React.FC = () => {
  const store = React.useContext(context).scannerStore;
  React.useEffect(
    () => {
      if (store) {
        store.scanningStore.startWatching();
        return () => store.scanningStore.stopWatching();
      }
    },
    [store],
  );
  if (!store) {
    return <Skeleton active />;
  }
  return (
    <div className={styles.root}>
      <Sources store={store} />
      <Divider />
      <Scanner store={store.scanningStore} />
      <ScanningPanel store={store.scanningStore} />
    </div>
  );
};

type ScannerProps = {
  readonly store: ScanningStore;
};

const Scanner: React.FC<ScannerProps> = ({ store }) => {
  const isScanning = useVal(store.$.isScanning);
  const isInterrupting = useVal(store.$.isInterrupting);
  const onClickScan = React.useCallback(
    () => store.scan(),
    [store],
  );
  const onClickInterrupt = React.useCallback(
    () => store.interrupt(),
    [store],
  );
  return <>
    <Typography>
      <Title>扫描</Title>
      <Paragraph>
        当知识库中的文件发生变化后，手动扫描以同步。
      </Paragraph>
    </Typography>
    <Button
      type="primary"
      shape="round"
      size="large"
      className={styles["scan-button"]}
      disabled={isScanning}
      loading={isScanning}
      icon={<ScanOutlined />}
      onClick={onClickScan} >
      扫  描
    </Button>
    {isScanning && (
      <Button
        shape="round"
        size="large"
        className={styles["scan-button"]}
        disabled={isInterrupting}
        loading={isInterrupting}
        icon={<PauseOutlined />}
        onClick={onClickInterrupt} >
        中 断
      </Button>
    )}
  </>;
}

type ScanningPanelProps = {
  readonly store: ScanningStore;
};

const ScanningPanel: React.FC<ScanningPanelProps> = ({ store }) => {
  type Record = {
    readonly icon: React.ReactNode;
    readonly title: string;
    readonly content: string;
    readonly loading: boolean;
  };
  const records: Record[] = [];
  const phase = useVal(store.$.phase);
  const scanCount = useVal(store.$.scanCount);
  const completedFiles = useVal(store.$.completedFiles);
  const handlingFile = useVal(store.$.handlingFile);
  const error = useVal(store.$.error);
  const isInterrupted = useVal(store.$.isInterrupted);

  let currentIndex: number;
  let status: "wait" | "process" | "finish" | "error" = "process";

  switch (phase) {
    case ScanningPhase.Ready: {
      return null;
    }
    case ScanningPhase.Scanning: {
      currentIndex = 0;
      break;
    }
    case ScanningPhase.HandingFiles: {
      currentIndex = 1;
      break;
    }
    case ScanningPhase.Completed: {
      currentIndex = 2;
      status = "finish";
      break;
    }
  }
  if (phase === ScanningPhase.Scanning) {
    records.push({
      icon: <ProfileTwoTone />,
      title: "扫描文件",
      content: `正在扫描文件的更新……`,
      loading: true,
    });
  } else {
    records.push({
      icon: <ProfileTwoTone />,
      title: "扫描文件",
      content: `扫描完成，发现 ${scanCount} 个文件有更新`,
      loading: false,
    });
  }
  for (const file of completedFiles) {
    records.push({
      icon: <FilePdfTwoTone />,
      title: `${textWithOperation(file.operation)} PDF 文件`,
      content: file.path,
      loading: false,
    });
  }
  if (handlingFile) {
    records.push({
      icon: <FilePdfTwoTone />,
      title: `${textWithOperation(handlingFile.operation)} PDF 文件`,
      content: handlingFile.path,
      loading: true,
    });
  }
  return <>
    <Steps
      className={styles["steps-bar"]}
      current={currentIndex}
      status={status}
      items={[
        { title: "扫描" },
        { title: "处理文件" },
        { title: "完成" },
      ]}
    />
    <List
      itemLayout="horizontal"
      dataSource={records}
      renderItem={item => (
        <List.Item>
          <List.Item.Meta
            avatar={item.icon}
            title={item.title}
            description={item.content}
          />
          {item.loading && (
            <SyncOutlined spin />
          )}
        </List.Item>
      )}
    />
    <ProgressBar
      name="解析"
      error={!!error || isInterrupted}
      pdfPage={handlingFile?.handlePdfPage} />
    <ProgressBar
      name="索引"
      error={!!error || isInterrupted}
      pdfPage={handlingFile?.indexPdfPage} />
    {error && (
      <Result
        status="error"
        title="扫描失败"
        subTitle={error}
      />
    )}
    {isInterrupted && (
      <Result
        status="info"
        title="扫描中断"
        subTitle="你可以继续使用知识库，但一些信息可能无法读取，另一些已失效的信息可能被错误读取。如果你希望知识库能正常运行，请继续扫描并等待完成。"
      />
    )}
  </>;
};

type ProgressBarProps = {
  readonly name: string;
  readonly error: boolean;
  readonly pdfPage?: {
    readonly index: number;
    readonly total: number;
  };
};

const ProgressBar: React.FC<ProgressBarProps> = ({ name, error, pdfPage }) => {
  if (!pdfPage) {
    return null;
  }
  const { index, total } = pdfPage;
  const percent = Math.floor(Math.min(index / total, 1.0) * 100);
  const status = error ? "exception" : (percent === 100 ? "success" : "active");
  return (
    <div className={styles["progress"]}>
      <label className={styles["progress-label"]}>
        {name}
      </label>
      <Progress
        className={styles["progress-bar"]}
        percent={percent}
        size="small"
        status={status} />
    </div>
  );
};

function textWithOperation(operation: FileOperation): string {
  let text: string = "";
  switch (operation) {
    case "create": {
      text = "录入";
      break;
    }
    case "update": {
      text = "更新";
      break;
    }
    case "remove": {
      text = "删除";
      break;
    }
  }
  return text;
}