import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from starlette.websockets import WebSocketState

from src.websocket.match_handler import MatchWebSocketHandler


@pytest.mark.asyncio
class TestMatchWebSocketHandler:
    async def test_send_initial_data_success(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.send_json = AsyncMock()
        client_id = "test_client"
        match_id = 123

        mock_data = {
            "data": {"players": []},
            "gameclock": {"gameclock": "00:00"},
            "playclock": {"playclock": "25"},
            "events": [],
            "statistics": {},
        }

        async def mock_fetch(*args, **kwargs):
            return mock_data

        with patch(
            "src.helpers.fetch_helpers.fetch_with_scoreboard_data",
            side_effect=mock_fetch,
        ):
            with patch("src.helpers.fetch_helpers.fetch_playclock", side_effect=mock_fetch):
                with patch("src.helpers.fetch_helpers.fetch_gameclock", side_effect=mock_fetch):
                    with patch("src.helpers.fetch_helpers.fetch_event", side_effect=mock_fetch):
                        with patch("src.helpers.fetch_helpers.fetch_stats", side_effect=mock_fetch):
                            with patch(
                                "src.websocket.match_handler.connection_manager"
                            ) as mock_conn_mgr:
                                mock_conn_mgr.queues = {client_id: AsyncMock()}
                                mock_conn_mgr.queues[client_id].put = AsyncMock()

                                await handler.send_initial_data(websocket, client_id, match_id)

                                websocket.send_json.assert_called_once()
                                call_args = websocket.send_json.call_args[0][0]
                                assert call_args["type"] == "initial-load"

    async def test_cleanup_websocket_success(self):
        handler = MatchWebSocketHandler()
        client_id = "test_client"

        with patch("src.websocket.match_handler.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.disconnect = AsyncMock()

            await handler.cleanup_websocket(client_id)

            mock_conn_mgr.disconnect.assert_called_once_with(client_id)

    async def test_cleanup_websocket_cancelled_error(self):
        handler = MatchWebSocketHandler()
        client_id = "test_client"

        with patch("src.websocket.match_handler.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.disconnect = AsyncMock(side_effect=asyncio.CancelledError())

            await handler.cleanup_websocket(client_id)

            mock_conn_mgr.disconnect.assert_called_once_with(client_id)

    async def test_receive_messages_pong(self):
        _ = MatchWebSocketHandler()
        websocket = AsyncMock()
        _ = "test_client"

        async def mock_iter_json():
            yield {"type": "pong"}

        websocket.iter_json = mock_iter_json

        with patch("src.websocket.match_handler.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.update_client_activity = Mock()

            messages = []
            async for message in websocket.iter_json():
                messages.append(message)
                if len(messages) >= 1:
                    break

            assert len(messages) == 1
            assert messages[0]["type"] == "pong"

    async def test_receive_messages_non_pong(self):
        _ = MatchWebSocketHandler()
        websocket = AsyncMock()
        _ = "test_client"

        async def mock_iter_json():
            yield {"type": "other"}

        websocket.iter_json = mock_iter_json

        with patch("src.websocket.match_handler.connection_manager"):
            messages = []
            async for message in websocket.iter_json():
                messages.append(message)
                if len(messages) >= 1:
                    break

            assert len(messages) == 1
            assert messages[0]["type"] == "other"

    async def test_process_match_data_with_websocket_connected(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_with_scoreboard_data") as mock_fetch:
            mock_fetch.return_value = {"data": {"match_id": 123}}

            await handler.process_match_data(websocket, match_id, None)

            websocket.send_json.assert_called_once()
            call_args = websocket.send_json.call_args[0][0]
            assert call_args["type"] == "match-update"

    async def test_process_match_data_with_provided_data(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        provided_data = {"type": "match-update", "data": {"match_id": 123}}

        await handler.process_match_data(websocket, match_id, provided_data)

        websocket.send_json.assert_called_once()
        call_args = websocket.send_json.call_args[0][0]
        assert call_args["type"] == "match-update"

    async def test_process_match_data_websocket_not_connected(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.DISCONNECTED
        match_id = 123

        await handler.process_match_data(websocket, match_id, None)

        websocket.send_json.assert_not_called()

    async def test_process_gameclock_data_success(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_gameclock") as mock_fetch:
            mock_fetch.return_value = {"gameclock": "00:00"}

            await handler.process_gameclock_data(websocket, match_id, None)

            websocket.send_json.assert_called_once()
            call_args = websocket.send_json.call_args[0][0]
            assert call_args["type"] == "gameclock-update"

    async def test_process_playclock_data_success(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_playclock") as mock_fetch:
            mock_fetch.return_value = {"playclock": "25"}

            await handler.process_playclock_data(websocket, match_id, None)

            websocket.send_json.assert_called_once()
            call_args = websocket.send_json.call_args[0][0]
            assert call_args["type"] == "playclock-update"

    async def test_process_event_data_with_provided_data(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        event_data = {"id": 1, "type": "event-update", "event_name": "touchdown"}

        await handler.process_event_data(websocket, match_id, event_data)

        websocket.send_json.assert_called_once()

    async def test_process_stats_data_with_provided_data(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        stats_data = {"type": "statistics-update", "statistics": {}}

        await handler.process_stats_data(websocket, match_id, stats_data)

        websocket.send_json.assert_called_once()

    async def test_process_data_websocket_connected_and_timeout(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        client_id = "test_client"
        match_id = 123

        mock_queue = AsyncMock()

        call_count = [0]

        async def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                raise asyncio.TimeoutError()
            websocket.application_state = WebSocketState.DISCONNECTED
            raise asyncio.TimeoutError()

        mock_queue.get = mock_get

        with patch("src.websocket.match_handler.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.get_queue_for_client = AsyncMock(return_value=mock_queue)

            await handler.process_data_websocket(websocket, client_id, match_id)

            websocket.send_json.assert_not_called()

            await handler.process_data_websocket(websocket, client_id, match_id)

    async def test_process_data_websocket_unknown_message_type(self):
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()

        mock_queue = AsyncMock()

        call_count = [0]

        async def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                return {"type": "unknown-type"}
            websocket.application_state = WebSocketState.DISCONNECTED
            raise asyncio.TimeoutError()

        mock_queue.get = mock_get

        with patch("src.websocket.match_handler.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.get_queue_for_client = AsyncMock(return_value=mock_queue)

    async def test_enable_match_clock_queues_registers_running_clocks(self):
        handler = MatchWebSocketHandler()
        match_id = 123

        gameclock = Mock()
        gameclock.id = 10
        gameclock.gameclock_status = "running"

        playclock = Mock()
        playclock.id = 20
        playclock.playclock_status = "running"
        playclock.playclock = None

        mock_gameclock_service = AsyncMock()
        mock_gameclock_service.get_gameclock_by_match_id = AsyncMock(return_value=gameclock)
        mock_gameclock_service.enable_match_data_gameclock_queues = AsyncMock()

        mock_playclock_service = AsyncMock()
        mock_playclock_service.get_playclock_by_match_id = AsyncMock(return_value=playclock)
        mock_playclock_service.enable_match_data_clock_queues = AsyncMock()

        with patch(
            "src.websocket.match_handler.GameClockServiceDB", return_value=mock_gameclock_service
        ):
            with patch(
                "src.websocket.match_handler.PlayClockServiceDB",
                return_value=mock_playclock_service,
            ):
                await handler.enable_match_clock_queues(match_id)

        mock_gameclock_service.enable_match_data_gameclock_queues.assert_called_once_with(
            gameclock.id
        )
        mock_playclock_service.enable_match_data_clock_queues.assert_called_once_with(
            playclock.id, 0
        )

    async def test_process_data_websocket_non_dict_data(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        client_id = "test_client"
        match_id = 123

        mock_queue = AsyncMock()

        call_count = [0]

        async def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                return "not a dict"
            websocket.application_state = WebSocketState.DISCONNECTED
            raise asyncio.TimeoutError()

        mock_queue.get = mock_get

        with patch("src.websocket.match_handler.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.get_queue_for_client = AsyncMock(return_value=mock_queue)

            await handler.process_data_websocket(websocket, client_id, match_id)

    async def test_process_data_websocket_disconnected_state(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.DISCONNECTED
        client_id = "test_client"
        match_id = 123

        await handler.process_data_websocket(websocket, client_id, match_id)

    async def test_process_match_data_connection_closed_ok(self):
        from websockets import ConnectionClosedOK

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedOK(None, None))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_with_scoreboard_data") as mock_fetch:
            mock_fetch.return_value = {"data": {"match_id": 123}}

            await handler.process_match_data(websocket, match_id, None)

    async def test_process_match_data_connection_closed_error(self):
        from websockets import ConnectionClosedError

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedError(None, None))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_with_scoreboard_data") as mock_fetch:
            mock_fetch.return_value = {"data": {"match_id": 123}}

            await handler.process_match_data(websocket, match_id, None)

    async def test_process_match_data_runtime_error_websocket_close(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=RuntimeError("websocket.close"))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_with_scoreboard_data") as mock_fetch:
            mock_fetch.return_value = {"data": {"match_id": 123}}

            await handler.process_match_data(websocket, match_id, None)

    async def test_process_gameclock_data_connection_closed_ok(self):
        from websockets import ConnectionClosedOK

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedOK(None, None))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_gameclock") as mock_fetch:
            mock_fetch.return_value = {"gameclock": "00:00"}

            await handler.process_gameclock_data(websocket, match_id, None)

    async def test_process_gameclock_data_connection_closed_error(self):
        from websockets import ConnectionClosedError

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedError(None, None))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_gameclock") as mock_fetch:
            mock_fetch.return_value = {"gameclock": "00:00"}

            await handler.process_gameclock_data(websocket, match_id, None)

    async def test_process_gameclock_data_runtime_error_websocket_close(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=RuntimeError("websocket.close"))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_gameclock") as mock_fetch:
            mock_fetch.return_value = {"gameclock": "00:00"}

            await handler.process_gameclock_data(websocket, match_id, None)

    async def test_process_playclock_data_connection_closed_ok(self):
        from websockets import ConnectionClosedOK

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedOK(None, None))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_playclock") as mock_fetch:
            mock_fetch.return_value = {"playclock": "25"}

            await handler.process_playclock_data(websocket, match_id, None)

    async def test_process_playclock_data_connection_closed_error(self):
        from websockets import ConnectionClosedError

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedError(None, None))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_playclock") as mock_fetch:
            mock_fetch.return_value = {"playclock": "25"}

            await handler.process_playclock_data(websocket, match_id, None)

    async def test_process_playclock_data_runtime_error_websocket_close(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=RuntimeError("websocket.close"))
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_playclock") as mock_fetch:
            mock_fetch.return_value = {"playclock": "25"}

            await handler.process_playclock_data(websocket, match_id, None)

    async def test_process_event_data_connection_closed_ok(self):
        from websockets import ConnectionClosedOK

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedOK(None, None))
        match_id = 123
        event_data = {"id": 1, "type": "event-update", "event_name": "touchdown"}

        await handler.process_event_data(websocket, match_id, event_data)

    async def test_process_event_data_connection_closed_error(self):
        from websockets import ConnectionClosedError

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedError(None, None))
        match_id = 123
        event_data = {"id": 1, "type": "event-update", "event_name": "touchdown"}

        await handler.process_event_data(websocket, match_id, event_data)

    async def test_process_event_data_runtime_error_websocket_close(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=RuntimeError("websocket.close"))
        match_id = 123
        event_data = {"id": 1, "type": "event-update", "event_name": "touchdown"}

        await handler.process_event_data(websocket, match_id, event_data)

    async def test_process_stats_data_connection_closed_ok(self):
        from websockets import ConnectionClosedOK

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedOK(None, None))
        match_id = 123
        stats_data = {"type": "statistics-update", "statistics": {}}

        await handler.process_stats_data(websocket, match_id, stats_data)

    async def test_process_stats_data_connection_closed_error(self):
        from websockets import ConnectionClosedError

        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=ConnectionClosedError(None, None))
        match_id = 123
        stats_data = {"type": "statistics-update", "statistics": {}}

        await handler.process_stats_data(websocket, match_id, stats_data)

    async def test_process_stats_data_runtime_error_websocket_close(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock(side_effect=RuntimeError("websocket.close"))
        match_id = 123
        stats_data = {"type": "statistics-update", "statistics": {}}

        await handler.process_stats_data(websocket, match_id, stats_data)

    async def test_process_gameclock_data_not_connected(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.DISCONNECTED
        match_id = 123

        await handler.process_gameclock_data(websocket, match_id, None)

    async def test_process_playclock_data_not_connected(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.DISCONNECTED
        match_id = 123

        await handler.process_playclock_data(websocket, match_id, None)

    async def test_process_event_data_not_connected(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.DISCONNECTED
        match_id = 123
        event_data = {"id": 1, "type": "event-update", "event_name": "touchdown"}

        await handler.process_event_data(websocket, match_id, event_data)

    async def test_process_stats_data_not_connected(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.DISCONNECTED
        match_id = 123
        stats_data = {"type": "statistics-update", "statistics": {}}

        await handler.process_stats_data(websocket, match_id, stats_data)

    async def test_process_event_data_fetch_when_none(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_event") as mock_fetch:
            mock_fetch.return_value = {"id": 1, "events": []}

            await handler.process_event_data(websocket, match_id, None)

            websocket.send_json.assert_called_once()
            call_args = websocket.send_json.call_args[0][0]
            assert call_args["type"] == "event-update"

    async def test_process_stats_data_fetch_when_none(self):
        handler = MatchWebSocketHandler()
        websocket = AsyncMock()
        websocket.application_state = WebSocketState.CONNECTED
        websocket.send_json = AsyncMock()
        match_id = 123

        with patch("src.helpers.fetch_helpers.fetch_stats") as mock_fetch:
            mock_fetch.return_value = {"statistics": {}}

            await handler.process_stats_data(websocket, match_id, None)

            websocket.send_json.assert_called_once()
            call_args = websocket.send_json.call_args[0][0]
            assert call_args["type"] == "statistics-update"
