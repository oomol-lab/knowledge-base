export class FetchError extends Error {
  public readonly statusCode: number;
  public readonly description?: string;

  public static is(error: unknown): error is FetchError {
    return (error as any).name == "FetchError";
  }

  constructor(message: string, statusCode: number, description?: string) {
    super(message);
    this.name = "FetchError";
    this.statusCode = statusCode;
    this.description = description;
  }
}

export async function fetchJson<T = unknown>(input: RequestInfo | URL, init: RequestInit = {}): Promise<T> {
  const [result, error] = await fetchJsonWithError<T>(input, init);
  if (error) {
    throw error;
  } else {
    return result;
  }
}

export async function fetchJsonWithError<T = unknown>(input: RequestInfo | URL, init: RequestInit = {}): Promise<[T, Error | null]> {
  const headers: Record<string, string> = {
    ...(init.headers as Record<string, string>),
    ["content-type"]: "application/json",
    ["accept"]: "application/json",
  };
  const method = init.method?.toUpperCase() || "GET";
  let wannaStatusCode: number = 200;
  switch (method) {
    case "POST": {
      wannaStatusCode = 201;
      break;
    }
    case "DELETE": {
      wannaStatusCode = 204;
      break;
    }
  }
  let response: Response;
  try {
    response = await fetch(input, { ...init, headers });
    if (response.status === wannaStatusCode) {
      if (response.status === 204) {
        return [null as T, null];
      } else {
        return [await response.json(), null];
      }
    }
    throw await parseError(response);

  } catch (error) {
    return [null as T, error as Error];
  }
}

export async function parseError(response: Response): Promise<Error> {
  const contentType = response.headers.get("content-type");
  if (contentType && contentType.includes("application/json")) {
    const { error, description } = await response.json();
    return new FetchError(error, response.status, description);
  } else {
    return new Error(`Unexpected response status ${response.status}`);
  }
}

export interface EventFetcher<T> {
  get(): Promise<T | null>;
  close(): void;
}

export function fetchJsonEvents<T>(url: string): EventFetcher<T> {
  let didClosed = false;
  const source = new EventSource(url);
  const events: T[] = [];
  const pendingList: [(r: T | null) => void, (e: unknown) => void][] = [];
  const close = () => {
    if (didClosed) {
      return;
    }
    didClosed = true;
    source.close();
    for (const [resolve, _] of pendingList.splice(0)) {
      resolve(null);
    }
  };
  const get = (): Promise<T | null> => {
    if (didClosed) {
      return Promise.resolve(null);
    } else if (events.length > 0) {
      return Promise.resolve(events.shift()!);
    } else {
      return new Promise((resolve, reject) => {
        pendingList.push([resolve, reject]);
      });
    }
  };
  const onCatchError = (error: unknown) => {
    for (const [_, reject] of pendingList.splice(0)) {
      reject(error);
    }
    source.close();
  };
  source.onmessage = (e) => {
    try {
      const event: T = JSON.parse(e.data);
      if (pendingList.length > 0) {
        const [resolve] = pendingList.shift()!;
        resolve(event);
      } else {
        events.push(event);
      }
    } catch (error) {
      onCatchError(error);
    }
  };
  source.onerror = (error) => {
    if (source.readyState === EventSource.CLOSED) {
      console.log("Connection has been closed");
    }
    onCatchError(error);
  };
  return { get, close };
}