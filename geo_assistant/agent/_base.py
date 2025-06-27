import inspect
import json
import openai
from typing import Callable, Any

from geo_assistant.config import Configuration
from geo_assistant.logging import get_logger

from geo_assistant.agent.updates import AiUpdate, Status, EmitUpdate, ToolUpdate

logger = get_logger(__name__)


class SystemMessageNotDeclared(Exception):
    def __init__(self):
        super().__init__("System message not declared on agent. One function must use `@system_message`")


async def _safe_run(fn, *args, **kwargs):
    """
    Helper function to always run a function, async or not
    """
    if inspect.iscoroutinefunction(fn):
        result = await fn(*args, **kwargs)
    else:
        result = fn(*args, **kwargs)
    return result


def system_message(fn):
    fn._is_system_message = True
    return fn


def model_params(fn):
    fn._is_model_params = True
    return fn

def prechat(fn):
    fn.is_prechat = True
    return fn

def postchat(fn):
    fn.is_postchat = True
    return fn


def tool(
    *,
    name: str = None,
    description: str = None,
    params: dict[str, dict] = None,
    required: list[str] = None,
    preprocess: Callable[[dict], dict] = None,
    postprocess: Callable[[Any], Any] = None,
):
    """
    Decorator to mark a method as a callable tool.
    """
    def decorator(fn: Callable):
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


def tool_type(
    *,
    name: str | None = None,
    description: str | None = None
):
    """
    Decorator to register a dynamic object type for tools.
    The builder function should return a dict of JSON-Schema properties.
    """
    def decorator(fn: Callable[[Any, str], dict[str, dict]]):
        type_name = name or fn.__name__
        fn._tool_type_meta = {
            "name":        type_name,
            "description": description or fn.__doc__ or "",
            "builder_fn":  fn,
        }
        # Register on the base class registry
        BaseAgent._tool_type_registry[type_name] = fn
        return fn
    return decorator


class BaseAgent:
    """
    Base agent class to be extended in order to define a new Agent

    Agent defintion requires the use of special function decorators in order to define the
        behavior of the agent.
    
        - @system_message: A mandatory function defining the system message. This is dynamic,
            and will be called on each iteration of `chat`.
        - @prechat: A function that will preprocess the user's message before sending it through
            the pipeline
        - @postchat: A function that will process the ai_message before returning it
        - @tool: Declares a method as a tool, allowing the agent to use it

    Agents also have default arguements that can be declared on initiation

    Args:
        - model (str): The OpenAI model that will be used for inference. Defaults to value
            found in `geo_assistant.config`
        - emitter (Callable): A function that will be called to recieve intermittant information
            about the agent's process. A common use case is that of a websocket, to be able
            to recieve information about the process as it is happening
    """
    _tool_type_registry: dict[str, Callable] = {}

    def __init__(
        self,
        model: str = Configuration.inference_model,
        emitter: Callable[[AiUpdate], None] = None
    ):
        self.client: openai.AsyncOpenAI = openai.AsyncOpenAI(api_key=Configuration.openai_key)
        self.model: str = model
        self.emitter = emitter
        self.messages: list[dict] = []

    async def _build_tool_defs(self, user_message: str) -> list[dict]:
        # 1) Build dynamic types from @tool_type
        definitions: dict[str, dict] = {}
        for type_name, builder in self._tool_type_registry.items():
            props = await _safe_run(builder, self, user_message)
            definitions[type_name] = {
                "type":        "object",
                "description": builder._tool_type_meta["description"],
                "properties":  props,
                "required":    [],
            }

        # 2) Build each @tool function schema
        tool_defs: list[dict] = []
        for attr in dir(self.__class__):
            fn = getattr(self.__class__, attr)
            if not hasattr(fn, "_tool_meta"):
                continue
            meta = fn._tool_meta

            # track which definitions this tool actually uses
            used_defs: set[str] = set()
            props: dict[str, dict] = {}
            for pname, spec in meta["params"].items():
                s = spec.copy()
                if callable(s.get("enum")):
                    s["enum"] = s["enum"](self)
                # shorthand: "type":"#foo"
                if isinstance(s.get("type"), str) and s["type"].startswith("#"):
                    ref_name = s["type"][1:]
                    used_defs.add(ref_name)
                    s = {"$ref": f"#/definitions/{ref_name}"}
                # handle array items referencing a type
                if s.get("type") == "array" and isinstance(s.get("items"), dict):
                    item = s["items"].copy()
                    if isinstance(item.get("type"), str) and item["type"].startswith("#"):
                        ref = item["type"][1:]
                        used_defs.add(ref)
                        s["items"] = {"$ref": f"#/definitions/{ref}"}
                    elif item.get('type') == "string":
                        if callable(item.get("enum")):
                            s['items']["enum"] = item["enum"](self)
                props[pname] = s

            parameters = {
                "type":       "object",
                "properties": props,
                "required":   meta["required"],
            }
            # only include definitions actually used by this tool
            if used_defs:
                parameters["definitions"] = {
                    name: definitions[name]
                    for name in used_defs
                    if name in definitions
                }

            tool_defs.append({
                "type":        "function",
                "name":        meta["name"],
                "description": meta["description"],
                "parameters":  parameters,
            })

        return tool_defs

    @property
    def _prechat_func(self) -> Callable | None:
        for attr in dir(self.__class__):
            fn = getattr(self.__class__, attr)
            if hasattr(fn, "_is_prechat"):
                return fn
        return None

    @property
    def _postchat_func(self) -> Callable | None:
        for attr in dir(self.__class__):
            fn = getattr(self.__class__, attr)
            if hasattr(fn, "_is_postchat"):
                return fn
        return None

    async def _build_system_message(self, user_message: str) -> str:
        for attr in dir(self.__class__):
            fn = getattr(self.__class__, attr)
            if hasattr(fn, "_is_system_message"):
                return await _safe_run(fn, self, user_message)
        raise SystemMessageNotDeclared()


    async def chat(self, user_message: str) -> str:
        # First, run a prechat processing function if one was given
        if self._prechat_func:
            user_message = await _safe_run(self._prechat_func, self, user_message)

        # Generate and insert the new system message
        system_message = {"role": "developer", "content": await self._build_system_message(user_message)}
        if self.messages:
            self.messages[0] = system_message
        else:
            self.messages.append(system_message)
        self.messages.append({"role":"user","content":user_message})

        # Create the tool list
        tool_defs = await self._build_tool_defs(user_message)
        print(tool_defs)

        # Begin the first pass on generating a response from openai
        if self.emitter:
            await _safe_run(
                self.emitter,
                EmitUpdate(
                    status=Status.GENERATING,
                )
            )
        try:
            response = await self.client.responses.create(
                model=self.model,
                input=self.messages,
                tools=tool_defs,
            )
        except Exception as e:
            logger.exception(e)
            # On failure, emit an udpate, update the messages, and return a standard message
            if self.emitter:
                await _safe_run(
                    self.emitter,
                    EmitUpdate(
                        status=Status.ERROR,
                    )
                )
            self.messages.append(
                {'role': 'assistant', 'content': 'Failed to generate a response'}
            )
            return f"OpenAI failed to generate a response: {e}"

        # Dispatch any tool calls
        made_calls = False
        for tool_call in response.output:
            if tool_call.type != "function_call":
                continue
            self.messages.append(tool_call)
            made_calls = True
            # Lookup the bound method
            handler = getattr(self, tool_call.name)
            kwargs  = json.loads(tool_call.arguments)

            logger.info(f"Calling {tool_call.name} with kwargs: {kwargs}")

            # Run the tool, emitting updates
            try:
                if self.emitter:
                    await _safe_run(
                    self.emitter,
                        ToolUpdate(
                                status=Status.PROCESSING,
                                tool_call=tool_call.name,
                                tool_args=kwargs
                            )
                    )
                result = await _safe_run(handler, **kwargs)
            except Exception as e:
                logger.exception(e)
                result = f"Tool `{tool_call.name}` failed: {e}. Please kindly state to the user that is failed, provide context, and ask if they want to try again."
                if self.emitter:
                    await _safe_run(
                        self.emitter,
                        ToolUpdate(
                                status=Status.ERROR,
                                tool_call=tool_call.name,
                                tool_args=kwargs
                            )
                    )

            # Record output for LLM
            self.messages.append({
                "type": "function_call_output",
                "call_id": tool_call.call_id,
                "output": result
            })

        # If tools ran, re-invoke LLM for natural reply
        if made_calls:
            try:
                response = await self.client.responses.create(
                    model=self.model,
                    input=self.messages,
                )
            except Exception as e:
                logger.exception(e)
                if self.emitter:
                    await _safe_run(
                        self.emitter,
                        EmitUpdate(
                            status=Status.ERROR,
                            message="Generation failed"
                        )
                    )
                self.messages.append(
                    {'role': 'assistant', 'content': 'Failed to generate a response'}
                )
                return f"OpenAI failed to generate a response: {e}"

        # Parse and finalize the Ai Response
        ai_message = response.output_text
        if self._postchat_func:
            ai_message = await _safe_run(self._postchat_func, self, ai_message)
        self.messages.append({'role': 'assistant', 'content': ai_message})

        if self.emitter:
            await _safe_run(
                self.emitter,
                AiUpdate(
                    status=Status.SUCCEDED,
                    message=ai_message
                )
            )

        return ai_message