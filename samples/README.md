# v1 样本

`v1/normal/00000000000000000000.seg` 由固定时钟下的服务接收两条真实 PRODUCE 请求后生成。话题为 `orders`，分区为 0。第一条的生产序号为 40、内容为 `received`，第二条序号为 41、内容为 `packed`。

`v1/interrupted/00000000000000000000.seg` 与正常样本同源，截去了第二条记录 CRC 的最后三个字节，使第二条记录成为物理上不完整的尾部。只读模式应报错。恢复副本只能读到偏移 0。让代理从中断样本开始，再用原生产身份重发第二条后，段文件应与完整样本逐字节一致。

`v1/hello.request.bin` 是送入 TCP 连接的 v1 协商请求。`v1/hello.response.bin` 保存服务返回的原始成功帧。

```bash
bash scripts/build.sh
java -cp build/tidemq.jar tidemq.SegmentMain verify orders 0 samples/v1/normal
java -cp build/tidemq.jar tidemq.SegmentMain recover-copy orders 0 \
  samples/v1/interrupted/00000000000000000000.seg /tmp/tidemq-recovered
bash scripts/samples.sh /tmp/tidemq-regenerated
diff -r samples/v1 /tmp/tidemq-regenerated
```

恢复目录和重生成目录应为新路径。`scripts/test.sh` 会再生成样本并逐字节比较，还会向损坏的完整记录写入错误校验并检查恢复被拒绝。
