import { val, derive, combine, Val, ReadonlyVal } from "value-enhancer";
import { message } from "antd";
import { fetchJson } from "../utils";

export type QueryStore$ = {
  readonly isQuerying: ReadonlyVal<boolean>;
  readonly items: ReadonlyVal<(readonly QueryItem[]) | null>;
  readonly keywords: ReadonlyVal<readonly QueryKeyword[]>;
  readonly lastQueryText: ReadonlyVal<string | null>;
  readonly resultsLimit: Val<number | null>;
};

export type QueryItem = PDFMetadataItem | PDFPageItem;
export type QueryKeyword = {
  readonly name: string;
  readonly checked: boolean;
};

export type PDFMetadataItem = {
  readonly pdf_files: readonly string[];
  readonly distance: number;
  readonly metadata: {
    readonly author: string | null;
    readonly modified_at: string | null;
    readonly producer: string | null;
  };
};

export type PDFPageItem = {
  readonly pdf_files: readonly {
    readonly scope: string;
    readonly path: string;
    readonly device_path: string;
    readonly page_index: number;
  }[];
  readonly distance: number;
  readonly content: string;
  readonly segments: readonly HighlightSegment[];
  readonly annotations: readonly {
    readonly index: number;
    readonly distance: number;
    readonly content: string;
    readonly segments: readonly HighlightSegment[];
  }[];
};

export type HighlightSegment = {
  readonly start: number;
  readonly end: number;
  readonly main: boolean;
  readonly highlights: readonly [number, number][];
};

type QueryResult = {
  readonly keywords: readonly string[];
  readonly items: readonly QueryItem[];
};

export class QueryStore {
  public readonly $: QueryStore$;

  readonly #isQuerying$: Val<boolean> = val(false);
  readonly #queryResult$: Val<QueryResult | null> = val<QueryResult | null>(null);
  readonly #checkedTags$: Val<Set<string>> = val<Set<string>>(new Set());
  readonly #lastQueryText: Val<string | null> = val<string | null>(null);
  readonly #resultsLimit: Val<number | null> = val<number | null>(10);

  #lastResultsLimit: number = -1;

  public constructor() {
    this.$ = {
      isQuerying: derive(this.#isQuerying$),
      items: derive(this.#queryResult$, r => r?.items ?? null),
      lastQueryText: derive(this.#lastQueryText),
      resultsLimit: this.#resultsLimit,
      keywords: combine(
        [this.#queryResult$, this.#checkedTags$],
        ([result, checkedTags]) => {
          const keywords: QueryKeyword[] = [];
          if(result)  {
            for (const keyword of result.keywords) {
              keywords.push({
                name: keyword,
                checked: checkedTags.has(keyword),
              });
            }
          }
          return keywords;
        },
      ),
    };
  }

  public checkTag(keyword: string, checked: boolean): void {
    if (this.#checkedTags$.value.has(keyword) === checked) {
      return;
    }
    const checkedTags = new Set(this.#checkedTags$.value);
    if (checked) {
      checkedTags.add(keyword);
    } else {
      checkedTags.delete(keyword);
    }
    this.#checkedTags$.set(checkedTags);
  }

  public query(text: string, resultsLimit: number): void {
    if (this.#lastQueryText.value === text &&
        this.#lastResultsLimit === resultsLimit) {
      return;
    }
    const query = new URLSearchParams({
      query: text,
      resultsLimit: `${resultsLimit}`,
    });
    this.#lastQueryText.set(text);
    this.#lastResultsLimit = resultsLimit;
    this.#isQuerying$.set(true);

    fetchJson<QueryResult>(`/api/query?${query}`)
      .then((queryResults) => {
        this.#queryResult$.set(queryResults);
        this.#checkedTags$.set(new Set(queryResults.keywords));
      })
      .catch((error) => {
        console.error(error);
        message.error(error.message);
      })
      .finally(() => {
        this.#isQuerying$.set(false);
      });
  }

  public cleanQuery(): void {
    this.#queryResult$.set(null);
    this.#checkedTags$.set(new Set());
    this.#lastQueryText.set(null);
    this.#lastResultsLimit = -1;
  }

}