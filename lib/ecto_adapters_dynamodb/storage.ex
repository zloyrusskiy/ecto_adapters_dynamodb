defmodule Ecto.Adapters.DynamoDB.Storage do
    @behaviour Ecto.Adapter.Storage

    def __using__(_opts) do
    end

    @impl true
    def storage_down(options) do
        :ok
    end

    @impl true
    def storage_status(options) do
        # DynamoDB does not require database creation
        :up
    end

    @impl true
    def storage_up(options) do
        :ok
    end
end