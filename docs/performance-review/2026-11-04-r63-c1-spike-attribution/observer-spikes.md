
## o1（B）

- 客户端 15.23ms；envelope total_ns=15.06ms；envelope spans: 
  - open 0.34ms（前间隙 0.01ms）：native.wait=0.29ms, admission.cpu=0.01ms, terminal.io=0.01ms
  - statement 0.02ms（前间隙 0.00ms）：admission.cpu=0.02ms, decode.cpu=0.00ms, audit.io=0.00ms
  - close 8.00ms（前间隙 6.67ms）：intent.io=7.89ms, admission.cpu=0.03ms, terminal.io=0.03ms
  - 末条之后间隙 0.01ms
- 客户端 8.66ms；envelope total_ns=8.56ms；envelope spans: 
  - open 8.46ms（前间隙 0.01ms）：terminal.io=8.16ms, native.wait=0.27ms, admission.cpu=0.01ms
  - statement 0.03ms（前间隙 0.01ms）：admission.cpu=0.02ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.00ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

## o2（A）

- 客户端 18.56ms；envelope total_ns=18.46ms；envelope spans: 
  - open 0.43ms（前间隙 17.95ms）：native.wait=0.36ms, admission.cpu=0.03ms, terminal.io=0.01ms
  - statement 0.03ms（前间隙 0.00ms）：admission.cpu=0.02ms, decode.cpu=0.00ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.00ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

## o3（A）

- 客户端 15.55ms；envelope total_ns=15.43ms；envelope spans: 
  - open 7.68ms（前间隙 0.01ms）：terminal.io=7.39ms, native.wait=0.25ms, admission.cpu=0.01ms
  - statement 7.67ms（前间隙 0.01ms）：admission.cpu=7.66ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.01ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

## o4（B）

- 客户端 14.78ms；envelope total_ns=14.66ms；envelope spans: 
  - open 0.30ms（前间隙 0.01ms）：native.wait=0.26ms, admission.cpu=0.01ms, intent.io=0.01ms
  - statement 14.27ms（前间隙 0.00ms）：admission.cpu=14.25ms, audit.io=0.01ms, decode.cpu=0.00ms
  - close 0.06ms（前间隙 0.01ms）：terminal.io=0.03ms, admission.cpu=0.01ms, intent.io=0.01ms
  - 末条之后间隙 0.01ms
- 客户端 11.54ms；envelope total_ns=11.44ms；envelope spans: 
  - open 11.35ms（前间隙 0.01ms）：terminal.io=11.08ms, native.wait=0.24ms, admission.cpu=0.01ms
  - statement 0.03ms（前间隙 0.01ms）：admission.cpu=0.02ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.00ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

## o5（B）

- 客户端 11.59ms；envelope total_ns=8.45ms；envelope spans: 
  - open 0.48ms（前间隙 7.83ms）：native.wait=0.37ms, admission.cpu=0.02ms, terminal.io=0.02ms
  - statement 0.05ms（前间隙 0.02ms）：admission.cpu=0.03ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.07ms（前间隙 0.01ms）：terminal.io=0.04ms, intent.io=0.01ms, respond.io=0.01ms
  - 末条之后间隙 0.01ms
- 客户端 10.89ms；envelope total_ns=4.68ms；envelope spans: 
  - open 0.53ms（前间隙 4.05ms）：native.wait=0.45ms, admission.cpu=0.03ms, terminal.io=0.01ms
  - statement 0.03ms（前间隙 0.01ms）：admission.cpu=0.02ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.01ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.01ms
- 客户端 10.72ms；envelope total_ns=10.63ms；envelope spans: 
  - open 0.37ms（前间隙 10.19ms）：native.wait=0.31ms, decode.cpu=0.01ms, admission.cpu=0.01ms
  - statement 0.03ms（前间隙 0.00ms）：admission.cpu=0.02ms, decode.cpu=0.00ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.00ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

## o6（A）

- 客户端 15.52ms；envelope total_ns=15.38ms；envelope spans: 
  - open 8.37ms（前间隙 0.01ms）：terminal.io=8.06ms, native.wait=0.28ms, admission.cpu=0.01ms
  - statement 6.94ms（前间隙 0.01ms）：admission.cpu=6.92ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.05ms（前间隙 0.01ms）：terminal.io=0.03ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.01ms

## o7（A）

- 客户端 14.63ms；envelope total_ns=14.51ms；envelope spans: 
  - open 6.52ms（前间隙 0.01ms）：terminal.io=6.20ms, native.wait=0.28ms, admission.cpu=0.01ms
  - statement 7.92ms（前间隙 0.01ms）：admission.cpu=7.90ms, decode.cpu=0.00ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.01ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

## o8（B）

- 客户端 7.57ms；envelope total_ns=7.46ms；envelope spans: 
  - open 0.39ms（前间隙 7.00ms）：native.wait=0.33ms, admission.cpu=0.01ms, terminal.io=0.01ms
  - statement 0.03ms（前间隙 0.01ms）：admission.cpu=0.02ms, decode.cpu=0.00ms, audit.io=0.00ms
  - close 0.04ms（前间隙 0.00ms）：terminal.io=0.02ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms
- 客户端 19.51ms；envelope total_ns=19.39ms；envelope spans: 
  - open 11.36ms（前间隙 0.01ms）：terminal.io=11.05ms, native.wait=0.27ms, admission.cpu=0.01ms
  - statement 7.96ms（前间隙 0.01ms）：admission.cpu=7.95ms, decode.cpu=0.01ms, audit.io=0.00ms
  - close 0.05ms（前间隙 0.01ms）：terminal.io=0.03ms, intent.io=0.01ms, admission.cpu=0.00ms
  - 末条之后间隙 0.00ms

注：观察器会放大请求计数并压低吞吐；本文件只用于归因，不作为性能数字。
