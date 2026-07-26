defmodule Zaq.Agent.AnsweringRun do
  @moduledoc """
  Answer post-processing shared by streaming transports.
  """

  @source_marker ~r/\s*\[\[source:[^\]]+\]\]/u

  @doc """
  Removes inline source markers duplicated by the structured `zaq_sources` frame.
  """
  @spec clean_answer(term()) :: String.t()
  def clean_answer(text) when is_binary(text) do
    text
    |> String.replace(@source_marker, "")
    |> String.replace(~r/[ \t]+\n/u, "\n")
    |> String.trim()
  end

  def clean_answer(nil), do: ""
  def clean_answer(other), do: other |> to_string() |> clean_answer()
end
