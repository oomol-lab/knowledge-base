import React from "react";
import styles from "./Link.module.less";

import { Tag, Tooltip } from "antd";
import { FilePdfFilled } from "@ant-design/icons";

export type PDFTagLinkProps = {
  readonly scope: string;
  readonly scopePath: string;
  readonly devicePath: string;
  readonly page: number;
};

export const PDFTagLink: React.FC<PDFTagLinkProps> = ({ scope, scopePath, devicePath, page }) => {
  const fileNames = devicePath.split(/[/\\]/);
  const fileName = fileNames[fileNames.length - 1];
  if (!fileName) {
    return null;
  }
  const fileNameWithoutExt = fileName.replace(/\.[^.]+$/, "");
  return (
    <Tag
      icon={<FilePdfFilled color="#FFFFFF" />}
      color="#FF5502">
      <Tooltip title={devicePath}>
        <a
          className={styles["pdf-a"]}
          href={`/files/${scope}${scopePath}#page=${page + 1}`}
          type="application/pdf"
          target="_blank">
          {fileNameWithoutExt}
        </a>
      </Tooltip>
      <span className={styles["pdf-page"]}>
        第 {page + 1} 页
      </span>
    </Tag>
  );
};