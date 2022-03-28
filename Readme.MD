GCN Data Pipeline
---
Test Task
____

What does this application?
--

1) Download files from Google Drive
2) Read, Parse, Filter downloaded files<br>
2.1) According to Specification: eans - filtering and data - files
3) Calculate Discount
4) Upload Data into DB

___

Application Structure
---
Python: 3.10<br>
PostgreSQL: 14.2<br>
Docker<br>
Docker-Compose

---
Installation & Run
----

    git clone https://github.com/Asfarial/gcn.git
    cd gcn
    docker-compose up --build --abort-on-container-exit

---
TODO
---

- Add some Unittests