import React from "react";
import styles from "./Sources.module.less";

import { Space, Input, Button, InputRef, Modal, Popover, Typography } from "antd";
import { EditTwoTone, DeleteTwoTone, PlusCircleTwoTone, WarningTwoTone } from "@ant-design/icons";
import { useVal } from "use-value-enhancer";
import { ScannerStore, SourceStore } from "../store";

const { Title, Paragraph } = Typography;

export type SourcesProps = {
  readonly store: ScannerStore;
};

export const Sources: React.FC<SourcesProps> = ({ store }) => {
  const sources = useVal(store.$.sources);
  return (
    <div className={styles.sources}>
      <Typography>
        <Title>知识库路径</Title>
        <Paragraph>
          只有知识库中的文件能被搜索到，在此界面添加你的知识库。左侧为知识库名（自行命名），右侧为文件夹路径。你可以添加多个知识库。
        </Paragraph>
      </Typography>
      {sources.map((store, index) => (
        <SourceItem
          key={`${index}`}
          store={store} />
      ))}
      <SourceAddition store={store} />
    </div>
  );
};

type SourceItemProps = {
  readonly store: SourceStore;
};

const SourceItem: React.FC<SourceItemProps> = ({ store }) => {
  const path = useVal(store.$.path);
  const isSubmitting = useVal(store.$.isSubmitting);
  const modified = useVal(store.$.modified);
  const canSubmitPath = useVal(store.$.canSubmitPath);
  const inputRef = React.useRef<InputRef>(null);
  const onChangeInput = React.useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => store.$.path.set(event.target.value),
    [store.$.path],
  );
  const onClickInput = React.useCallback(
    () => inputRef.current?.focus({ cursor: "all" }),
    [inputRef.current],
  );
  const onPressEnter = React.useCallback(
    (event: React.KeyboardEvent<HTMLInputElement>) => {
      if (canSubmitPath) {
        event.preventDefault();
        store.submitPath();
      }
    },
    [canSubmitPath, store],
  );
  const onClickRemove = React.useCallback(
    () => {
      Modal.confirm({
        icon: <DeleteTwoTone />,
        title: "确认删除",
        content: `是否删除“${store.name}”？`,
        onOk: () => store.remove(),
      });
    },
    [store],
  );
  return (
    <div className={styles["source-editor"]}>
      <Input
        className={styles["source-input"]}
        ref={inputRef}
        addonBefore={store.name}
        value={path}
        disabled={isSubmitting}
        status={modified ? "warning" : ""}
        onPressEnter={onPressEnter}
        onChange={onChangeInput}
        onClick={onClickInput} />
      <Button
        className={styles["source-button"]}
        icon={<EditTwoTone />}
        disabled={!canSubmitPath || isSubmitting}
        onClick={() => store.submitPath()}>
        提交
      </Button>
      <Button
        className={styles["source-button"]}
        icon={<DeleteTwoTone />}
        disabled={isSubmitting}
        onClick={onClickRemove}>
        删除
      </Button>
    </div>
  );
};

type SourceAdditionProps = {
  readonly store: ScannerStore;
}

const SourceAddition: React.FC<SourceAdditionProps> = ({ store }) => {
  const pathInputRef = React.useRef<InputRef>(null);
  const addedSourceName = useVal(store.$.addedSourceName);
  const addedSourcePath = useVal(store.$.addedSourcePath);
  const isSubmittingAddition = useVal(store.$.isSubmittingAddition);
  const isAddedNameDuplicated = useVal(store.$.isAddedNameDuplicated);
  const canAdd = useVal(store.$.canAdd);
  const onChangeNameInput = React.useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => store.$.addedSourceName.set(event.target.value),
    [store.$.addedSourceName],
  );
  const onChangePathInput = React.useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => store.$.addedSourcePath.set(event.target.value),
    [store.$.addedSourcePath],
  );
  const onClickPathInput = React.useCallback(
    () => pathInputRef.current?.focus({ cursor: "all" }),
    [pathInputRef.current],
  );
  const onPressPathEnter = React.useCallback(
    (event: React.KeyboardEvent<HTMLInputElement>) => {
      if (canAdd) {
        event.preventDefault();
        store.addSource();
      }
    },
    [canAdd, store],
  );
  const onClickAdd = React.useCallback(
    () => store.addSource(),
    [store],
  );
  return (
    <div className={styles["source-editor"]}>
      <Space.Compact className={styles["source-input"]}>
        <Popover
          open={isAddedNameDuplicated}
          placement="bottomLeft"
          content={<>
            <WarningTwoTone
              className={styles["warn-icon"]}
              twoToneColor="#FF636C" />
            该名字已存在！
          </>}>
          <Input
            placeholder="名字"
            className={styles["source-input-name"]}
            value={addedSourceName}
            disabled={isSubmittingAddition}
            status={isAddedNameDuplicated ? "error" : ""}
            onChange={onChangeNameInput} />
        </Popover>
        <Input
          placeholder="知识库文件夹路径"
          ref={pathInputRef}
          value={addedSourcePath}
          disabled={isSubmittingAddition}
          onChange={onChangePathInput}
          onPressEnter={onPressPathEnter}
          onClick={onClickPathInput} />
      </Space.Compact>
      <Button
        className={styles["source-button"]}
        type="primary"
        icon={<PlusCircleTwoTone />}
        disabled={!canAdd || isSubmittingAddition}
        onClick={onClickAdd} >
        添加
      </Button>
    </div>
  );
};