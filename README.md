<p align="center">
  <img src="banner.png" alt="GeoAssistant Banner" width="800"/>
</p>


GeoAssistant is an interactive, LLM-powered GIS data explorer that enables users to query, filter, and visualize geospatial parcel data through natural language. Leveraging the NYC PLUTO database, GeoAssistant demonstrates how vector-stores and AI-guided tools can simplify complex spatial queries and mapping workflows.

## Key Features

* **Natural Language Filtering**: Specify filters like zoning, borough, or land use in plain English; GeoAssistant translates your request into structured API calls.
* **Dynamic Map Visualization**: Instantly render filtered parcels on a Plotly Dash map with seamless zoom, pan, and layer control.
* **Scalable Spatial Backend**: Powered by PostGIS for geospatial queries and pg-tileserv for on-the-fly vector tiles.
* **Extensible Architecture**: Modular components for the AI engine, data ingestion, and frontend allow easy customization and integration.

## Project Architecture

1. **Data Layer**:

   * **PostGIS**: Stores the PLUTO dataset with geometry indexes for fast spatial queries.
   * **pg-tileserv**: Serves vector tiles directly from PostGIS for efficient map rendering.

2. **AI Layer**:

   * **Vector Store**: FAISS index of field definitions extracted from the PLUTO documentation.
   * **LLM Integration**: GPT-driven module interprets user queries, retrieves relevant field metadata, and formulates filter parameters.

3. **Application Layer**:

   * **Dash Frontend**: Interactive UI built with Plotly Dash and Bootstrap components.
   * **API Server**: Uvicorn/Starlette-based service exposing endpoints for both map data and conversational AI.

## Getting Started

### Prerequisites

* Docker
* Docker Compose
* (Optional) Local installation of PostgreSQL & PostGIS for advanced development

### Production Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/your-org/geoassistant.git
   cd geoassistant
   ```
2. Launch all services:

   ```bash
   docker-compose --profile prod up -d
   ```
3. Access the application at:
   `http://127.0.0.1:8050/`
4. To shut down:

   ```bash
   docker-compose --profile prod down
   ```

### Development Workflow

For rapid iteration without rebuilding the Dash container:

1. Start backend services only:

   ```bash
   docker-compose --profile dev up -d
   ```
2. Run the Dash app locally (requires Python 3.9+ and dependencies):

   ```bash
   pip install -r requirements.txt
   python3 -m geo_assistant.app
   ```
3. Start the React frontend with Vite:

   ```bash
   cd frontend
   npm install
   npm run dev
   ```
4. Tail service logs:

   ```bash
   docker-compose logs -f geo_assistant  # AI + API server
   docker-compose logs -f tileserv       # Vector tile server
   docker-compose logs -f db             # PostGIS database
   ```

## Configuration

* **Environment Variables**:

  * `DATABASE_URL` – Connection string for PostGIS database.
  * `TILESERVER_URL` – URL for the pg-tileserv endpoint.
  * `OPENAI_API_KEY` – API key for the LLM.

* **Parcel Loading**
    Right now the application was built to use the NYC Pluto dataset in mind. To load the database with that data, run the load_parcels module *after* the database is up and running.

    ```bash
    python3 -m geo_assistant.load_parcels
    ```

## Contributing

1. Fork the repository and create a feature branch.
2. Run tests and ensure code style compliance.
3. Submit a pull request describing your changes.

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgements

* NYC Department of City Planning – PLUTO Dataset
* Plotly Dash – Web application framework
* pg-tileserv – Vector tile server for PostGIS
* OpenAI – GPT models for natural language processing
