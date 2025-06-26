# Design Document: Lightweight LLM Tool Dispatch Framework

---

## Table of Contents

1. [Introduction](#introduction)  
2. [Motivation & Goals](#motivation--goals)  
3. [High-Level Architecture](#high-level-architecture)  
4. [Core Components](#core-components)  
   - [4.1. `@tool` Decorator](#41-tool-decorator)  
   - [4.2. Dynamic Parameter Resolution](#42-dynamic-parameter-resolution)  
   - [4.3. Base `Agent` Class](#43-base-agent-class)  
   - [4.4. Chat & Dispatch Flow](#44-chat--dispatch-flow)  
5. [Example: `GeoAgent` Implementation](#example-geoagent-implementation)  
6. [Incremental Refactor Path](#incremental-refactor-path)  
7. [Testing & Validation](#testing--validation)  
8. [Future Extensions](#future-extensions)  
9. [References & Further Reading](#references--further-reading)  

---

## Introduction

Modern conversational interfaces often need to augment pure language understanding with **structured tool calls**—for example, to query a database, manipulate a map, or run an analysis routine. OpenAI’s function-calling feature lets us define “tools” with JSON schemas and let the model decide when and how to invoke them. However, wiring up tool definitions, dynamic parameter lists, error handling, and post-processing can quickly lead to large amounts of repetitive boilerplate.

This document presents a **lightweight, pragmatic framework** that:

- Centralizes all chat and tool-dispatch logic in one base class  
- Lets you declare individual tools simply by decorating methods  
- Supports dynamic parameter values (e.g. enums drawn from runtime state)  
- Keeps your agent subclasses focused on domain logic, not plumbing  

By following this design, you can add or remove LLM‐callable tools in your agent by editing **just one method** decorated with `@tool`, without touching the chat loop or dispatch code.

---

## Motivation & Goals

1. **Reduce Boilerplate**  
   - Eliminate long `if/elif` chains for each tool name  
   - Collapse argument parsing, error catching, async/sync dispatch into a single loop  

2. **Ease of Extension**  
   - Enable contributors to add new tools by writing a method + decorator  
   - No need to manually update tool schemas in multiple places  

3. **Dynamic Configuration**  
   - Allow certain parameters (like `enum` lists) to be computed at runtime based on agent state  
   - Keep static schema pieces (types, descriptions) co-located with the tool logic  

4. **Separation of Concerns**  
   - Core LLM‐and‐dispatch logic lives in a reusable base `Agent`  
   - Domain-specific agents (e.g. `GeoAgent`) only define tools and override a few hooks  

5. **Minimal Dependencies**  
   - Use standard library (`inspect`, `json`) and simple callable conventions  
   - Avoid heavy metaprogramming or external schema libraries  

---

## High-Level Architecture

```

+-------------------+          +----------------------+
\|   Domain Agent    |          |    Base Agent        |
\| (e.g. GeoAgent)   | <------> |  (orchestrator)      |
\|                   |  @tool   |                      |
\|  - tool methods   |  ------> | - chat()             |
\|  - optional overrides       | - \_build\_tool\_defs()  |
\|                   |          | - \_dispatch\_tools()  |
+-------------------+          +----------------------+

````

- **Domain Agent**  
  - Subclasses the base `Agent`  
  - Declares tools via `@tool` on methods  
  - Overrides optional hooks for system messages or final formatting  

- **Base Agent**  
  - Manages message history, OpenAI calls, and tool dispatch  
  - Scans for `@tool` metadata on its subclass to build function‐schemas  
  - Resolves dynamic enums or parameter callables at runtime  
  - Contains one central `chat()` method that handles the entire turn  

---

## Core Components

### 4.1. `@tool` Decorator

A minimal decorator that annotates a method with:

- `name` (string): tool identifier  
- `description` (string): user-friendly description  
- `params` (dict): static JSON-schema fragments for each parameter  
- `required` (list): names of required parameters  
- `preprocess` (callable): optional argument transformer before invocation  
- `postprocess` (callable): optional result formatter after invocation  

```python
def tool(
    *,
    name: str = None,
    description: str = None,
    params: dict[str, dict] = None,
    required: list[str] = None,
    preprocess: Callable[[dict], dict] = None,
    postprocess: Callable[[Any], Any] = None,
):
    def decorator(fn):
        fn._tool_meta = {
            "name":        name or fn.__name__,
            "description": description or fn.__doc__ or "",
            "params":      params or {},
            "required":    required or [],
            "pre":         preprocess,
            "post":        postprocess,
            "method":      fn.__name__,
        }
        return fn
    return decorator
````

#### Static vs. Dynamic Schema Pieces

* **Static**: parameter types, default values, descriptions
* **Dynamic**: `enum` arrays can be **callables** that accept `self` and return a list

---

### 4.2. Dynamic Parameter Resolution

Within `params`, you may declare an `enum` entry as a lambda or function:

```python
params = {
  "table": {
    "type": "string",
    "enum": lambda self: [tbl.name for tbl in self.registry.tables]
  },
  "layer_id": {"type":"string"},
  // …
}
```

At runtime, the base `Agent._build_tool_defs()` inspects each `spec["enum"]`. If it’s callable, it invokes `spec["enum"](self)` to get the up-to-date list.

---

### 4.3. Base `Agent` Class

The heart of the framework is a single base class that any domain agent can inherit:

```python
class Agent:
    def __init__(self, client, model, socket_emit=None):
        self.client      = client
        self.model       = model
        self.socket_emit = socket_emit
        self.messages    = []

    def _system_messages(self) -> list[dict]:
        """Override to inject initial system prompts or context."""
        return []

    def _finalize_response(self, text: str) -> str:
        """Override to post-process the final assistant reply."""
        return text

    def _build_tool_defs(self) -> list[dict]:
        """Scan for @tool metadata, resolve dynamic enums, and emit JSON schemas."""
        defs = []
        for attr in dir(self.__class__):
            fn = getattr(self.__class__, attr, None)
            if not hasattr(fn, "_tool_meta"):
                continue
            meta = fn._tool_meta
            props = {}
            for pname, spec in meta["params"].items():
                spec = spec.copy()
                enum = spec.get("enum")
                if callable(enum):
                    spec["enum"] = enum(self)
                props[pname] = spec
            defs.append({
                "name":        meta["name"],
                "description": meta["description"],
                "parameters": {
                    "type":       "object",
                    "properties": props,
                    "required":   meta["required"],
                }
            })
        return defs

    async def chat(self, user_message: str) -> str:
        # 1. Prepend system messages & record user
        for msg in self._system_messages():
            self.messages.append(msg)
        self.messages.append({"role":"user","content":user_message})

        # 2. Call OpenAI with dynamic tools list
        tool_defs = self._build_tool_defs()
        response = await self.client.responses.create(
            model=self.model,
            input=self.messages,
            tools=tool_defs,
        )

        # 3. Dispatch any function calls
        made_calls = False
        for call in response.output:
            if call.type != "function_call":
                continue
            made_calls = True
            # Lookup the bound method
            handler = getattr(self, call.name)
            args    = json.loads(call.arguments)

            # Optional preprocessing
            if handler._tool_meta["pre"]:
                args = handler._tool_meta["pre"](args)

            # Invoke (sync or async)
            try:
                if inspect.iscoroutinefunction(handler):
                    result = await handler(**args)
                else:
                    result = handler(**args)
            except Exception as e:
                result = f"Tool `{call.name}` failed: {e}"

            # Optional postprocessing
            if handler._tool_meta["post"]:
                result = handler._tool_meta["post"](result)

            # Record output for LLM
            self.messages.append({
                "type":    "function_call_output",
                "call_id": call.call_id,
                "output":  result
            })

        # 4. If tools ran, re-invoke LLM for natural reply
        if made_calls:
            response = await self.client.responses.create(
                model=self.model,
                input=self.messages,
            )

        # 5. Finalize and return
        final_text = self._finalize_response(response.output_text)
        self.messages.append({'role':'assistant','content':final_text})
        if self.socket_emit:
            await self.socket_emit({"type":"ai_response","message":final_text})
        return final_text
```

---

### 4.4. Chat & Dispatch Flow

1. **System Messages**: optional preamble (context, dev notes) via `_system_messages()`
2. **User Message**: recorded into history
3. **Tool Definitions**: `_build_tool_defs()` resolves static + dynamic schema info
4. **LLM Call**: send `messages` + `tools` to OpenAI
5. **Function-Call Dispatch**: for each `tool_call` →

   * Lookup handler
   * Preprocess args
   * Invoke method
   * Postprocess result
   * Append output as LLM context
6. **Follow-up LLM Call**: if any tools ran, get a human-readable final reply
7. **Final Post-Processing**: optional tweaks via `_finalize_response()`
8. **Emit & Return**: send via socket if available, and return text

---

## Example: `GeoAgent` Implementation

```python
# geo_agent.py
from base import Agent, tool
from typing import List
from your_registry_module import TableRegistry
from your_filter_module import extract_filters

class GeoAgent(Agent):
    def __init__(self, client, model, engine, map_handler, data_handler, info_store, socket_emit=None):
        super().__init__(client, model, socket_emit)
        self.engine       = engine
        self.map_handler  = map_handler
        self.data_handler = data_handler
        self.info_store   = info_store
        self.registry     = None

    async def _system_messages(self):
        # load context and inject as system prompt
        self.registry = TableRegistry.load_from_tileserv(self.engine)
        context = await self.info_store.query(self.messages[-1]['content'], k=3)
        md      = "\n\n".join(r['markdown'] for r in context)
        return [{"role":"system","content":md}]

    def _finalize_response(self, text: str) -> str:
        return text.strip()

    @tool(
        name="add_map_layer",
        description="Add a layer to the map with optional CQL filters",
        params={
            "table":    {"type":"string", "enum": lambda self: [t.name for t in self.registry.tables]},
            "layer_id": {"type":"string"},
            "style":    {"type":"string"},
            "color":    {"type":"string"},
            "filters":  {"type":"array", "items":{"type":"object"}},
        },
        required=["table","layer_id"],
        preprocess=extract_filters,
        postprocess=lambda count: f"{count} parcels found",
    )
    def add_map_layer(self, table: str, filters: List[dict], style: str, color: str, layer_id: str) -> int:
        self.map_handler._add_map_layer(table=table, filters=filters, style=style, color=color)
        return self.data_handler.filter_count(self.engine, filters)

    @tool(
        name="remove_map_layer",
        description="Remove a layer by its ID",
        params={"layer_id":{"type":"string"}},
        required=["layer_id"],
    )
    def remove_map_layer(self, layer_id: str) -> bool:
        return self.map_handler.remove_layer(layer_id)

    @tool(
        name="run_analysis",
        description="Perform spatial analysis on the current selection",
        params={"prompt":{"type":"string"}},
        required=["prompt"],
    )
    async def run_analysis(self, prompt: str) -> str:
        # Hand off to specialized analysis logic or agent
        return await super().run_analysis(prompt)
```

---

## Incremental Refactor Path

If you already have a working agent with hard-coded tool calls, you can migrate in small steps:

1. **Extract Dispatch Map**

   ```python
   self._tool_map = {
     "add_map_layer":    (self._add_map_layer, preprocess, postprocess),
     "remove_map_layer": (self._remove_map_layer, None, None),
     # etc.
   }
   ```
2. **Replace `if/elif` with Generic Loop**

   ```python
   handler, pre, post = self._tool_map[call.name]
   # …same argument parsing & invocation…
   ```
3. **Introduce `@tool` Metadata**

   * Add the decorator to each method
   * Create `_build_tool_defs()` to read metadata instead of static list
4. **Simplify `chat()`**

   * Remove special-case branches
   * Call `_build_tool_defs()` + generic dispatch loop

By step 4, your `chat()` function is \~50 lines of orchestrator code, and each tool is declared in **one place only**.

---

## Testing & Validation

* **Unit Tests for `_build_tool_defs()`**

  * Verify that static `params` appear correctly
  * Verify that callable `enum` lambdas produce correct lists

* **Integration Tests for `chat()`**

  * Mock the OpenAI client to respond with a function call JSON
  * Ensure the right method is invoked with parsed arguments
  * Test error propagation when a tool raises an exception

* **End-to-End Scenarios**

  * Run GeoAgent against sample user prompts
  * Assert that map layers are added/removed properly
  * Confirm that the final LLM reply is coherent

---

## Future Extensions

* **Hook Decorator**

  * Add lightweight `@hook.system()` or `@hook.response()` decorators if you need more than two override methods

* **Type-Annotation Schema Inference**

  * Use `inspect.signature` and `typing.Annotated` to derive full parameter schemas from Python type hints

* **Multiple Agent Orchestration**

  * Build a registry of agents, so one tool can forward to another agent’s `chat()`

* **Telemetry & Metrics**

  * Track which tools are called most frequently
  * Measure latency and error rates per-tool

* **Automatic Documentation Generation**

  * Export the tool catalog to Markdown or Swagger/OpenAPI for human consumption

---

## References & Further Reading

* OpenAI Function Calling: [https://platform.openai.com/docs/guides/function-calling](https://platform.openai.com/docs/guides/function-calling)
* Python `inspect` Module: [https://docs.python.org/3/library/inspect.html](https://docs.python.org/3/library/inspect.html)
* PEP 593: Annotated Types (for future type-hint integration)

