// Types for the shared SQL failure contract module. The implementation is
// web/sqlcontract.js, which the browser clients load directly and the console
// bundles at build time.
export type FailureClass = "statement" | "uncertain" | "request" | "limit" | "access" | "session" | "capacity" | "placement" | "platform" | "unsupported" | "unrecognized";
export type Proof = "settled" | "rejected" | "unproven";

export declare const CODES: Record<string, FailureClass>;
export declare function classOf(code: string): FailureClass;
export declare function proofOf(completed: unknown, committed: unknown): Proof;
export declare function uncertain(failure: Record<string, unknown>): boolean;
export declare function unknown(failure: Record<string, unknown>, status: number): boolean;
export declare function executed(failure: Record<string, unknown>): boolean;
export declare function commitRisk(failure: Record<string, unknown>): boolean;
