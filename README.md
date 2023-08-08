# Kafka投递到PgSQL

这是一个Python应用程序，它从Kafka处理消息并将它们存储在PostgreSQL数据库中。

## 安装步骤

1. 克隆仓库
    ```
    git clone https://git.ycgame.com/ycgame/Data_Analysis/ycdata-kafka2pgsql.git
    ```

2. 构建Docker镜像
    ```
    docker build -t kafka2pgsql .
    ```

3. 运行Docker容器
    ```
    docker run  kafka2pgsql
    ```

## 使用说明

1、复制一个Data.Model类，把数据结构改成实际JSON结构。不要的字段可以不保留。

2、在main.py中按照模板复制一个对应名称的Data.Model函数，并把函数加入到safe_func中。

3、执行测试

4、Docker打包

## 配置

config.json为链接库配置

## 注意

数据库插入失败，仍然会消费掉kafka信息，需要查看日志信息从而知晓原因和解决问题。

## 贡献

如果你希望对此项目进行贡献，请阅读 [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) 获取详细的行为准则和提交拉取请求的过程。

## 许可

此项目采用MIT许可 - 更多详情请查阅 [LICENSE.md](LICENSE.md) 文件
