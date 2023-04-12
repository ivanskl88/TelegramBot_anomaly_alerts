Project name        |	Система алертов для нашего приложения      |
:---                |---        |
**Description**     |  система должна с периодичность каждые 15 минут проверять ключевые метрики, такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений  |
**Target**          | в случае обнаружения аномального значения, в чат должен отправиться алерт - сообщение со следующей информацией: метрика, ее значение, величина отклонения       |
**Stack**           | :heavy_check_mark: **Python** `pandahouse ` `airflow` `telegram` `io` `matplotlib` `seaborn`  </br> :heavy_check_mark: **ClickHouse** </br> :heavy_check_mark: **Airflow**          |

---

### Пример отчета

![2023-04-11_20-05-01](https://user-images.githubusercontent.com/110673529/231441313-3803bbdb-b0dd-44ba-bb46-a923a63168a5.png)

---

### DAG в Airflow

![2023-04-11_20-08-17](https://user-images.githubusercontent.com/110673529/231441329-5bc35d57-b411-4191-9192-079ea9959391.png)
