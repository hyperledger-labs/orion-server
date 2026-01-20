export interface FetchOptions {
  body?: any;
  headers?: Record<string, string>;
}

export interface RequestOptions {
  method: string;
  headers: Record<string, string>;
  body?: any;
}
