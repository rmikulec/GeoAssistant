# System Architecture
This is a living document of the currently implemented architecture of GeoAssistant. Please update
this doc accordingly for any PR that changes it.

## Services

Three services are built / deployed via `docker-compose`

  1. PostGIS Database
  1. pg-tileserv Tile Server
  1. GeoAssistant Python Application

Below is a basic outline of how the services interact with each other

```mermaid
sequenceDiagram
    actor User
    User->>GeoAssistant: User Message
    GeoAssistant->>pg-tileserv: Tile Request with Filters
    pg-tileserv->>PostGIS: Request for Metadata
    PostGIS->>pg-tileserv: TileData
    pg-tileserv->>GeoAssistant: Vector Tiles
    GeoAssistant->>PostGIS: Query
    PostGIS->>GeoAssistant: Macro Stats on Filters
    GeoAssistant->>User: Updated Map and Response
```

### PostGIS

This service is built via docker-compose, and lives within its own Docker Container

### pg-tileserv

This service is also built via docker-compose, and lives in its own container.

More information can be found on the [Github Page](https://github.com/CrunchyData/pg_tileserv) or [Official Documentation](https://access.crunchydata.com/documentation/pg_tileserv/latest/)


### GeoAssistant

This is the main application that carries the core logic. This application is split into several key components, including (not limited to for simplicity):
  - `DashApp`: Frontend application that accepts data from user, displays the map, and displays the chat log
  - `FieldDefinitionStore`: Vector store of field definitions parsed from the PLUTO data dictionary. Used to dynamically build map-filter tools.
  - `SupplementalInfoStore`: Vector store for any additional appendix or lookup-table information to provide extra context for the model.
  - `GeoAgent`: Holds the core logic for calling OpenAI SDK, routing tool calls, managing messages and orchestrating `run_analysis` workflows.
  - `MapHandler`: Manages the plotly map object by adding *layers* and updating the figured with those. Directly interacts with `pg-tileserv`
  - `DataHandler`: Wrapper class to easily query the PostGIS database


Below is a quick sequence diagram on how these systems interact

```mermaid
%%{init: { "theme": "base", "themeVariables": { "sequenceNumberColor": "#999" }}}%%
sequenceDiagram
  %% Define colored swimlanes
  box "User Facing" #D6EAF8
    actor User
    participant DashApp as "Dash App"
  end

  box "Agent Pipeline" #AED6F1
    participant FieldDefStore as "Field Definition Store"
    participant SuppInfoStore as "Supplemental Info Store"
    participant GeoAgent
    participant MapHandler as "Map Handler"
    participant DataHandler as "Data Handler"
  end

  box "External Services" #85C1E9
    participant OpenAI
  end

  %% Main flow with activations
  User->>DashApp: User Message
  activate DashApp

  GeoAgent->>FieldDefStore: Query for field definitions
  activate FieldDefStore
  FieldDefStore-->>GeoAgent: Relevant Field Definitions
  deactivate FieldDefStore

  GeoAgent->>SuppInfoStore: Query for supplemental info
  activate SuppInfoStore
  SuppInfoStore-->>GeoAgent: Contextual Information
  deactivate SuppInfoStore

  deactivate DashApp
  DashApp->>GeoAgent: Forward message, definitions & context
  activate GeoAgent

  GeoAgent->>OpenAI: Queries with tools, context, and user message
  activate OpenAI
  OpenAI-->>GeoAgent: Response with potential tool calls
  deactivate OpenAI

  loop Every OpenAI Tool Call
    alt run_analysis called
      GeoAgent->>OpenAI: Plan analysis steps
      activate OpenAI
      OpenAI-->>GeoAgent: Analysis plan
      deactivate OpenAI

      loop Every Analysis Step
        GeoAgent->>DataHandler: Create analysis tables
        activate DataHandler
        DataHandler-->>GeoAgent: Table created
        deactivate DataHandler
      end

    else other tool calls
      GeoAgent->>MapHandler: Updates map based on tool call
      activate MapHandler
      MapHandler-->>GeoAgent: Map updated
      deactivate MapHandler

      GeoAgent->>DataHandler: Query for relevant information
      activate DataHandler
      DataHandler-->>GeoAgent: Info for user
      deactivate DataHandler
    end
  end

  GeoAgent->>DashApp: Send updated map
  GeoAgent->>DashApp: Send AI response
  deactivate GeoAgent

  DashApp->>User: Display map and new message
```
