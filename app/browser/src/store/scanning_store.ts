import { val, derive, combine, Val, ReadonlyVal } from "value-enhancer";
import { message } from "antd";
import { fetchJson, fetchJsonEvents, EventFetcher } from "../utils";

type Event = {
  readonly kind: (
    "scanning" |
    "completed" |
    "interrupting" |
    "interrupted" |
    "heartbeat"
  );
} | {
  readonly kind: "scanCompleted";
  readonly count: number;
} | {
  readonly kind: "startHandingFile";
  readonly path: string;
  readonly operation: FileOperation;
}| {
  readonly kind: "completeHandingFile";
  readonly path: string;
  readonly operation: FileOperation;
} | {
  readonly kind: "completeParsePdfPage";
  readonly index: number;
  readonly total: number;
} | {
  readonly kind: "completeIndexPdfPage";
  readonly index: number;
  readonly total: number;
} | {
  readonly kind: "failure";
  readonly error: string;
};

export type ScanningStore$ = {
  readonly phase: ReadonlyVal<ScanningPhase>;
  readonly scanCount: ReadonlyVal<number>;
  readonly handlingFile: ReadonlyVal<HandingFile | null>;
  readonly completedFiles: ReadonlyVal<readonly File[]>;
  readonly error: ReadonlyVal<string | null>;
  readonly isInterrupting: ReadonlyVal<boolean>;
  readonly isInterrupted: ReadonlyVal<boolean>;
  readonly isScanning: ReadonlyVal<boolean>;
};

export enum ScanningPhase {
  Ready,
  Scanning,
  HandingFiles,
  Completed,
}

export type File = {
  readonly path: string;
  readonly operation: FileOperation;
};

export type FileOperation = "create" | "update" | "remove";

export type HandingFile = File & {
  readonly handlePdfPage?: {
    readonly index: number;
    readonly total: number;
  };
  readonly indexPdfPage?: {
    readonly index: number;
    readonly total: number;
  };
};

export class ScanningStore {
  public readonly $: ScanningStore$;

  readonly #phase$: Val<ScanningPhase> = val(ScanningPhase.Ready);
  readonly #scanCount$: Val<number> = val(0);
  readonly #handlingFile$: Val<HandingFile | null> = val<HandingFile | null>(null);
  readonly #completedFiles$: Val<readonly File[]> = val<readonly File[]>([]);
  readonly #error$: Val<string | null> = val<string | null>(null);
  readonly #isInterrupting$: Val<boolean> = val(false);
  readonly #isInterrupted$: Val<boolean> = val(false);

  #fetcher: EventFetcher<Event> | null = null;

  public constructor() {
    this.$ = {
      phase: derive(this.#phase$),
      scanCount: derive(this.#scanCount$),
      handlingFile: derive(this.#handlingFile$),
      completedFiles: derive(this.#completedFiles$),
      error: derive(this.#error$),
      isInterrupting: derive(this.#isInterrupting$),
      isInterrupted: derive(this.#isInterrupted$),
      isScanning: combine(
        [this.#phase$, this.#error$, this.#isInterrupted$],
        ([phase, error, isInterrupted]) => {
          switch (phase) {
            case ScanningPhase.Ready:
            case ScanningPhase.Completed: {
              return false;
            }
          }
          if (error) {
            return false;
          }
          if (isInterrupted) {
            return false;
          }
          return true;
        },
      ),
    };
  }

  public async scan(): Promise<void> {
    await fetchJson("/api/scanning", { method: "POST" });
  }

  public async interrupt(): Promise<void> {
    await fetchJson("/api/scanning", { method: "DELETE" });
  }

  public startWatching(): void {
    if (this.#fetcher) {
      return;
    }
    this.#fetcher = fetchJsonEvents<Event>("/api/scanning");
    this.#runLoop(this.#fetcher).catch((error) => {
      console.error(error);
      message.error(error.message);
    }).finally(() => {
      this.#fetcher = null;
    });
  }

  public stopWatching(): void {
    if (!this.#fetcher) {
      return;
    }
    this.#fetcher.close();
    this.#fetcher = null;
  }

  async #runLoop(fetcher: EventFetcher<Event>): Promise<void> {
    while (true) {
      const event = await fetcher.get();
      if (!event) {
        break;
      }
      switch (event.kind) {
        case "scanning": {
          this.#phase$.set(ScanningPhase.Scanning);
          this.#scanCount$.set(0);
          this.#handlingFile$.set(null);
          this.#completedFiles$.set([]);
          this.#error$.set(null);
          this.#isInterrupted$.set(false);
          break;
        }
        case "scanCompleted": {
          this.#phase$.set(ScanningPhase.HandingFiles);
          this.#scanCount$.set(event.count);
          break;
        }
        case "startHandingFile": {
          this.#handlingFile$.set({
            path: event.path,
            operation: event.operation,
          });
          break;
        }
        case "completeHandingFile": {
          this.#handlingFile$.set(null);
          this.#completedFiles$.set([
            ...this.#completedFiles$.value,
            {
              path: event.path,
              operation: event.operation,
            },
          ]);
          break;
        }
        case "completeParsePdfPage": {
          this.#handlingFile$.set({
            ...this.#handlingFile$.value!,
            handlePdfPage: {
              index: event.index,
              total: event.total,
            },
          });
          break;
        }
        case "completeIndexPdfPage": {
          this.#handlingFile$.set({
            ...this.#handlingFile$.value!,
            indexPdfPage: {
              index: event.index,
              total: event.total,
            },
          });
          break;
        }
        case "completed": {
          this.#phase$.set(ScanningPhase.Completed);
          break;
        }
        case "failure": {
          this.#error$.set(event.error);
          break;
        }
        case "interrupting": {
          this.#isInterrupting$.set(true);
          break;
        }
        case "interrupted": {
          this.#isInterrupting$.set(false);
          this.#isInterrupted$.set(true);
          break;
        }
      }
    }
  }
}