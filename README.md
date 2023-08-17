#  Super-Pipe: EtLT Pipeline with Prefect, DuckDB, and GCP

The Cornershop EtLT Pipeline is designed to process data from 540 distinct products sourced from major supermarkets in Florianópolis. Leveraging the power of Prefect, DuckDB, and GCP, this pipeline ensures efficient data processing, accuracy in data representation, and scalability to handle growing data demands.

## Motivation
With a multitude of products from various supermarkets, there's a need to have a robust pipeline that can extract, transform, load, and then transform the data again, ensuring it's ready for analytics and insights derivation.
### **EtLT Components:**

![EtLT-map](https://kroki.io/graphviz/svg/eNptj01LxEAMhu_7K0JPLbigZ6lQ7SDCIKjjaRFJ27gdHGeWmXRZlf3vTls6fuAlCW_yJG86vfW46-EaPlfg0b522pfq8nwF1nUEm9DjjsrGHZ6iFIZmnm7NEJj88-lIGWzIlJlgqaDyba-ZWh48ZZEQsFna-YE9tlxk4yZOOufxanhx_q2ANdzGjEZ_IGtnp0mZJmXusJtxlcRcFQsf8Stn9-TDwgpYX8RTMcgxqCgd__ni7McXj4E83DtDYbRff_uvkRGE3WpL8cK0vq5-dyuL5j1wOIGHOwk17WNV3fyFJlMiVZwqOa9MRo9fJ8d9fQ==)

- **E(xtract):**
    
    This component interfaces directly with the Cornershop API, ensuring accurate and timely data extraction.
    
- **t(ransform):**
    
    Utilizing DuckDB, the data undergoes normalization processes, preparing it for the subsequent loading phase.
    
- **L(oad):**
    
    Orchestrated by Prefect, this phase ensures that the data is stored in a structured and optimized manner.
    
- **(T)ransform:**
    
    Another round of transformation is applied using DuckDB. Here, business rules are translated, turning the raw data into actionable insights that can drive business decisions.
    

### **GCP Integration:**

- **Artifact Registry:** Ensures consistent runtime environments by storing the Docker image.
- **Cloud Run:** Entrusted with executing the data processing scripts.
- **Compute Engine:** Acts as the host for the Prefect agent, which orchestrates the entire process.
- **Cloud Storage:** Serves as the central repository for daily data, ensuring data integrity and ease of access.
- **BigQuery:** A serverless data warehouse solution that facilitates analytics, making sense of the processed data.

### **Initialization via GitHub Action:**

- **Artifact Registry:** If no repository is present, one is initialized to manage versions and changes.
- **Docker Operations:** It takes charge of constructing a Docker image based on the Dockerfile and subsequently pushes it to the Artifact Registry.
- **VM Deployment:** Deploys a VM, overwriting existing VMs with the same name. For diverse requirements, the action can be executed with distinct VM/queue names.
- **Prefect Agent:** This component deploys a Docker container equipped with a Prefect agent, enabling flow runs. By default, it configures the flows as serverless containers via Google Cloud Run, offering flexibility and scalability. Any necessary adjustments in resource allocation can be done via the Prefect UI.
- **Prefect Blocks & Flows:** Both blocks and flows under Prefect are auto-deployed, streamlining the initialization process.


