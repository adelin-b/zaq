defmodule Zaq.Agent.ClientToolRun do
  @moduledoc """
  One answering turn whose LLM may also call tools that the **caller** executes
  (OpenAI Chat Completions `tools` / `tool_choice`).

  The Jido agent server can only expose `Jido.Action` modules to the LLM and has
  no way to pause a run until a caller returns tool results, so a request that
  carries client tools runs here instead, on ReqLLM directly, with the same
  model, credentials, sampling options and internal tools as the answering
  agent (`Zaq.Agent.Factory.runtime_config/1`, `Zaq.Agent.ProviderSpec.build/1`).

  The LLM sees ZAQ's internal tools and the caller's tools side by side:

    * internal tool calls (e.g. `search_knowledge_base`) run here, server-side,
      through the agent's tool interceptors and with the pipeline's permission
      context (`person_id`, `team_ids`, `source_filter`, `skip_permissions`);
      they are never returned to the caller;
    * the first LLM turn that calls a caller tool ends the run: its caller tool
      calls come back on `metadata.client_tool_calls` and the caller answers them
      with `role: "tool"` messages in its next request.

  Context is the conversation's persisted history (`Zaq.Agent.HistoryLoader`),
  then the user question, then the caller's `:tool_exchange` — the assistant
  `tool_calls` message(s) and `tool` results that followed that question, which
  ZAQ never stored. A turn that ends in client tool calls is therefore not
  persisted; only the final answer is (see `Zaq.Agent.Api`).
  """

  require Logger

  alias Jido.AI.Context, as: AIContext
  alias Jido.AI.{ToolAdapter, Turn, Usage}
  alias Zaq.Agent.{Answering, Factory, HistoryLoader, ProviderSpec, Status, StreamEvents}
  alias Zaq.Engine.Messages.{Incoming, Outgoing}
  alias Zaq.Identity.ExecutionActor

  @flush_interval_ms 100

  @typedoc "A caller tool from the request, already validated by the controller."
  @type client_tool :: %{name: String.t(), description: String.t(), parameters: map()}

  @doc """
  Runs the turn and returns the pipeline `%Outgoing{}`. Never raises.

  ## Options

    * `:client_tools` — `[client_tool()]`
    * `:tool_choice` — OpenAI `tool_choice` (`"auto" | "none" | "required" |
      %{"type" => "function", "function" => %{"name" => name}}`), or `nil`
    * `:tool_exchange` — `[%{role: :assistant, content:, tool_calls: [%{id, name,
      arguments}]} | %{role: :tool, tool_call_id:, name:, content:}]`
    * `:question`, `:person_id`, `:team_ids`, `:source_filter`, `:skip_permissions`,
      `:node_router` — as for `Zaq.Agent.Executor.run/2`

  The result carries the provider-reported token usage summed over the run's
  LLM calls (`prompt_tokens`, `completion_tokens`, `total_tokens`), like the
  executor's; the fields are absent when the provider reported none.
  """
  @spec run(Incoming.t(), keyword()) :: Outgoing.t()
  def run(%Incoming{} = incoming, opts) do
    agent = Answering.answering_configured_agent()

    result =
      with {:ok, actor} <- execution_actor(opts),
           {:ok, runtime} <- Factory.runtime_config(agent, actor: actor),
           {:ok, model} <- ProviderSpec.build(agent),
           {:ok, client_tools} <- reqllm_client_tools(Keyword.get(opts, :client_tools, [])),
           :ok <- ensure_distinct_names(client_tools, runtime.tools) do
        state = %{
          incoming: incoming,
          opts: opts,
          actor: actor,
          internal: Map.new(runtime.tools, &{&1.name(), &1}),
          client_names: MapSet.new(client_tools, & &1.name),
          llm_opts:
            runtime.llm_opts
            |> Keyword.put(:tools, ToolAdapter.from_actions(runtime.tools) ++ client_tools)
            |> put_tool_choice(Keyword.get(opts, :tool_choice)),
          model: model,
          context: initial_context(incoming, runtime, opts),
          tool_calls: [],
          usage: nil,
          max_iterations: agent.max_iterations || 10
        }

        loop(state, 1)
      end

    Outgoing.from_pipeline_result(incoming, to_result(result))
  end

  # Same identity the Executor binds the agent server to: the dispatching
  # event's actor, validated.
  defp execution_actor(opts) do
    case Keyword.get(opts, :event) do
      %Zaq.Event{actor: actor} -> ExecutionActor.validate(actor)
      _ -> ExecutionActor.validate(nil)
    end
  end

  # ---------------------------------------------------------------------------
  # Loop
  # ---------------------------------------------------------------------------

  defp loop(state, iteration) when iteration > state.max_iterations,
    do: {:error, :max_iterations_reached}

  defp loop(state, iteration) do
    messages = AIContext.to_messages(state.context)

    case stream_step(state.model, messages, state.llm_opts, &stream_delta(state, &1)) do
      {:ok, turn} ->
        state |> add_usage(turn) |> next(turn, iteration)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp next(state, %Turn{tool_calls: []} = turn, _iteration),
    do: {:ok, finished(state, turn, [])}

  defp next(state, %Turn{tool_calls: calls} = turn, iteration) do
    case Enum.split_with(calls, &MapSet.member?(state.client_names, &1.name)) do
      {[], internal_calls} ->
        state
        |> run_internal(turn, internal_calls)
        |> loop(iteration + 1)

      {client_calls, _internal_calls} ->
        # The caller must answer every call it receives before the model
        # continues, so internal calls issued in the same turn are dropped:
        # the model can issue them again after the tool results arrive.
        {:ok, finished(state, turn, client_calls)}
    end
  end

  defp finished(state, turn, client_calls) do
    %{
      answer: turn.text,
      tool_calls: state.tool_calls,
      client_tool_calls: client_calls,
      usage: state.usage
    }
  end

  # Provider-reported counts only; a turn without usage adds nothing.
  defp add_usage(state, %Turn{usage: %{} = usage}) when map_size(usage) > 0 do
    counts = Usage.token_counts(usage)
    %{state | usage: Map.merge(state.usage || %{}, counts, fn _key, a, b -> a + b end)}
  end

  defp add_usage(state, _turn), do: state

  defp run_internal(state, turn, calls) do
    context = AIContext.append_assistant(state.context, Turn.assistant_content(turn), calls)

    {context, records} =
      Enum.reduce(calls, {context, state.tool_calls}, fn call, {ctx, records} ->
        result = execute_internal(state, call)

        {AIContext.append_tool_result(
           ctx,
           call.id,
           call.name,
           Turn.format_tool_result_content(result)
         ), records ++ [tool_call_record(call, result)]}
      end)

    # `tool_choice` constrains the caller-visible turn only: once an internal
    # tool answered it, a repeated "required" or named choice would force
    # another call on every iteration.
    %{
      state
      | context: context,
        tool_calls: records,
        llm_opts: Keyword.delete(state.llm_opts, :tool_choice)
    }
  end

  defp execute_internal(state, call) do
    module = Map.fetch!(state.internal, call.name)
    context = tool_context(state)
    tool_call = %{id: call.id, name: call.name, arguments: call.arguments, action_module: module}

    with {:ok, tool_call} <- Factory.before_tool_call(tool_call, context),
         result <- Turn.execute_module(module, tool_call.arguments, context),
         {:ok, result} <- Factory.after_tool_call(tool_call, result, context) do
      result
    else
      {:error, reason} -> {:error, reason, []}
    end
  end

  # Same keys the Executor puts on the Jido tool context — retrieval reads its
  # permission scope from here.
  defp tool_context(%{incoming: incoming, opts: opts, actor: actor}) do
    %{
      incoming: incoming,
      actor: actor,
      person_id: Keyword.get(opts, :person_id),
      team_ids: Keyword.get(opts, :team_ids, []),
      source_filter: Keyword.get(opts, :source_filter),
      skip_permissions: Keyword.get(opts, :skip_permissions, false),
      node_router: Keyword.get(opts, :node_router, Zaq.NodeRouter),
      opaque_alias_scope: "client_tools:" <> to_string(incoming.message_id)
    }
  end

  # Shape `ZaqWeb.ChatCompletionsController` reads citations from, as recorded
  # by `Zaq.Agent.StreamEvents` for Jido runs.
  defp tool_call_record(call, result) do
    response =
      case result do
        {:ok, payload, _effects} -> payload
        {:error, reason, _effects} -> %{error: inspect(reason)}
      end

    StreamEvents.json_safe(%{
      "id" => call.id,
      "type" => "tool_call",
      "name" => call.name,
      "arguments" => call.arguments,
      "response" => response,
      "status" => "completed"
    })
  end

  # ---------------------------------------------------------------------------
  # Context
  # ---------------------------------------------------------------------------

  defp initial_context(incoming, runtime, opts) do
    question = Keyword.get(opts, :question) || incoming.content

    history =
      case incoming.metadata do
        %{conversation_id: id} when is_binary(id) -> HistoryLoader.load_for_conversation(id)
        _ -> AIContext.new()
      end

    %{history | system_prompt: blank_to_nil(runtime.system_prompt)}
    |> AIContext.append_user(question)
    |> append_exchange(Keyword.get(opts, :tool_exchange, []))
  end

  defp append_exchange(context, exchange) do
    Enum.reduce(exchange, context, fn
      %{role: :assistant, content: content, tool_calls: calls}, ctx ->
        AIContext.append_assistant(ctx, content || "", calls)

      %{role: :tool, tool_call_id: id, name: name, content: content}, ctx ->
        AIContext.append_tool_result(ctx, id, name, content)
    end)
  end

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

  # ---------------------------------------------------------------------------
  # LLM
  # ---------------------------------------------------------------------------

  defp reqllm_client_tools(tools) do
    Enum.reduce_while(tools, {:ok, []}, fn tool, {:ok, acc} ->
      case ReqLLM.Tool.new(
             name: tool.name,
             description: tool.description,
             parameter_schema: tool.parameters,
             callback: fn _args -> {:error, :client_tool} end
           ) do
        {:ok, reqllm_tool} -> {:cont, {:ok, acc ++ [reqllm_tool]}}
        {:error, reason} -> {:halt, {:error, {:invalid_client_tool, tool.name, reason}}}
      end
    end)
  end

  defp ensure_distinct_names(client_tools, internal_modules) do
    clashes =
      MapSet.intersection(
        MapSet.new(client_tools, & &1.name),
        MapSet.new(internal_modules, & &1.name())
      )

    if MapSet.size(clashes) == 0,
      do: :ok,
      else: {:error, {:reserved_tool_names, Enum.sort(clashes)}}
  end

  # OpenAI's shapes map onto ReqLLM's: strings pass through, a named function
  # becomes `%{type: "tool", name: name}`.
  defp put_tool_choice(opts, nil), do: opts

  defp put_tool_choice(opts, %{"function" => %{"name" => name}}),
    do: Keyword.put(opts, :tool_choice, %{type: "tool", name: name})

  defp put_tool_choice(opts, choice) when is_binary(choice),
    do: Keyword.put(opts, :tool_choice, choice)

  defp stream_step(model, messages, llm_opts, on_text) do
    with {:ok, response} <- ReqLLM.stream_text(model, messages, llm_opts),
         {:ok, response} <-
           ReqLLM.StreamResponse.to_response(%{
             response
             | stream: tap_text(response.stream, on_text)
           }) do
      {:ok, Turn.from_response(response)}
    end
  end

  # Calls `on_text` with the cumulative answer text, at most every
  # @flush_interval_ms; the final answer reconciles whatever was not flushed.
  defp tap_text(stream, on_text) do
    Stream.transform(stream, {"", nil}, fn
      %ReqLLM.StreamChunk{type: :content, text: text} = chunk, {acc, last} when is_binary(text) ->
        acc = acc <> text
        now = System.monotonic_time(:millisecond)

        # Monotonic time can be negative, so "never flushed" is nil, not 0.
        if is_nil(last) or now - last >= @flush_interval_ms do
          on_text.(acc)
          {[chunk], {acc, now}}
        else
          {[chunk], {acc, last}}
        end

      chunk, state ->
        {[chunk], state}
    end)
  end

  defp stream_delta(%{incoming: incoming, opts: opts}, cumulative) do
    Status.broadcast(
      incoming,
      :answering,
      cumulative,
      Keyword.get(opts, :node_router, Zaq.NodeRouter),
      update_intent: :stream_delta
    )

    :ok
  end

  # ---------------------------------------------------------------------------
  # Result
  # ---------------------------------------------------------------------------

  defp to_result({:ok, %{usage: usage} = result}) do
    result
    |> Map.delete(:usage)
    |> Map.merge(%{error: false, sources: []})
    |> Map.merge(token_fields(usage))
  end

  defp to_result({:error, reason}) do
    Logger.error("Client-tool run failed: #{inspect(reason)}")

    %{
      answer: "Sorry, something went wrong while generating the answer.",
      error: true,
      error_reason: inspect(reason),
      tool_calls: [],
      client_tool_calls: [],
      sources: []
    }
  end

  defp token_fields(%{input_tokens: prompt, output_tokens: completion, total_tokens: total}),
    do: %{prompt_tokens: prompt, completion_tokens: completion, total_tokens: total}

  defp token_fields(_usage), do: %{}
end
