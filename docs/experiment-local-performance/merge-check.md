# 提交前集成检查

当前 main 基线：`11ad02c222b1b256a0875ed92347b1dd619d5830`。在独立 lab 中用该基线加本轮 Rust 改动进行 release / locked / offline 编译及针对性回归。

结果：36 项通过，1 项手工计时测试按原标记忽略。覆盖 boot admission 4、Walker 4、branch/GC 6、WAL 14、storage 8，包括 main 已提交的大事务拆分与并发回放功能。源码摘要、编译日志及完整测试输出见 [merge-check.json](evidence/merge-check.json)、[构建产物记录](evidence/merge-check-build.jsonl)。

这是提交时的集成兼容检查，不是新的性能样本；2.396×结论仍只针对此前冻结的实验03候选。
