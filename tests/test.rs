use bitcoin_capnp_types::{
    chain_capnp::unstable_chain,
    rpc_capnp::rpc,
    init_capnp::init,
    mining_capnp,
    proxy_capnp::{thread, thread_map},
    chain_capnp::chain_notifications
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side};
use encoding::encode_to_vec;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::task::LocalSet;

mod util;

use serde_json::{Value, json};
use util::bitcoin_core::{
    connect_unix_stream, destroy_template, make_block_template, mempool_tx_count, unix_socket_path,
    make_chain, make_rpc, make_unstable_chain, with_init_client, with_mining_client, with_rpc_client,
    with_chain_client,
    with_unstable_chain_client,
};
use util::bitcoin_core_wallet::{
    bitcoin_test_wallet, create_mempool_self_transfer, ensure_wallet_loaded_and_funded,
    mine_blocks_to_new_address,
};
use util::block::{block_solution, block_with_pow};

struct SubmitOutcome {
    accepted: bool,
    reason: String,
    debug: String,
}

async fn submit_block(mining: &mining_capnp::mining::Client, block: &[u8]) -> SubmitOutcome {
    let mut req = mining.submit_block_request();
    req.get().set_block(block);
    let resp = req.send().promise.await.unwrap();
    let results = resp.get().unwrap();
    SubmitOutcome {
        accepted: results.get_result(),
        reason: results.get_reason().unwrap().to_string().unwrap(),
        debug: results.get_debug().unwrap().to_string().unwrap(),
    }
}

async fn submit_solution(
    template: &mining_capnp::block_template::Client,
    solution: &util::block::BlockSolution,
) -> SubmitOutcome {
    let mut req = template.submit_solution_request();
    {
        let mut params = req.get();
        params.set_version(solution.version);
        params.set_timestamp(solution.timestamp);
        params.set_nonce(solution.nonce);
        params.set_coinbase(&solution.coinbase);
    }
    let resp = req.send().promise.await.unwrap();
    let results = resp.get().unwrap();
    SubmitOutcome {
        accepted: results.get_result(),
        reason: results.get_reason().unwrap().to_string().unwrap(),
        debug: results.get_debug().unwrap().to_string().unwrap(),
    }
}

async fn get_template_block(template: &mining_capnp::block_template::Client) -> Vec<u8> {
    let resp = template.get_block_request().send().promise.await.unwrap();
    resp.get().unwrap().get_result().unwrap().to_vec()
}

#[tokio::test]
#[serial_test::parallel]
async fn integration() {
    with_init_client(|client| async move {
        let echo_client_request = client.make_echo_request().send().promise.await.unwrap();
        let echo_client = echo_client_request.get().unwrap().get_result().unwrap();
        let mut echo_conf = echo_client.echo_request();
        echo_conf.get().set_echo("Hello world");
        let echo_response = echo_conf.send().promise.await.unwrap();
        let text = echo_response
            .get()
            .unwrap()
            .get_result()
            .unwrap()
            .to_string()
            .unwrap();
        assert_eq!("Hello world", text);
    })
    .await;
}

/// Test the RPC interface by calling `uptime`
#[tokio::test]
#[serial_test::parallel]
async fn rpc_query_uptime() {
    with_rpc_client(|_client, rpc| async move {
        let mut execute_rpc_request = rpc.execute_rpc_request();
        let j: Value = json!({
            "jsonrpc": "2.0",
            "id": "test",
            "method": "uptime",
            "params": [],
        });
        execute_rpc_request.get().set_request(j.to_string());
        let exec_rpc_response = execute_rpc_request.send().promise.await.unwrap();
        let result = exec_rpc_response
            .get()
            .unwrap()
            .get_result()
            .unwrap()
            .to_string()
            .unwrap();
        let v: Value = serde_json::from_str(&result)
            .map_err(|e| format!("failed to parse rpc response as JSON: {e}"))
            .unwrap();
        let uptime = v["result"].as_i64().unwrap();
        assert!(uptime >= 0, "Uptime must not be negative");
    })
    .await;
}

/// Invalid JSON in an executeRpc request must be rejected, and the connection
/// should keep working afterwards.
#[tokio::test]
#[serial_test::parallel]
async fn rpc_invalid_json_rejected() {
    with_rpc_client(|_client, rpc| async move {
        let mut invalid_request = rpc.execute_rpc_request();
        invalid_request.get().set_request("invalid json");
        invalid_request.get().set_uri("/");
        let err = match invalid_request.send().promise.await {
            Ok(_) => panic!("invalid JSON should be rejected by the server"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("invalid JSON received over IPC"),
            "unexpected error: {err}"
        );

        // The connection still works after the failed call.
        let mut valid_request = rpc.execute_rpc_request();
        let j: Value = json!({
            "jsonrpc": "2.0",
            "id": "test",
            "method": "getblockcount",
            "params": [],
        });
        valid_request.get().set_request(j.to_string());
        valid_request.get().set_uri("/");
        let response = valid_request.send().promise.await.unwrap();
        let result = response
            .get()
            .unwrap()
            .get_result()
            .unwrap()
            .to_string()
            .unwrap();
        let v: Value = serde_json::from_str(&result).unwrap();
        assert!(
            v["result"].as_i64().is_some(),
            "getblockcount should return a number: {v}"
        );
    })
    .await;
}

/// Calling the deprecated makeMiningOld2 (@2) should return an error from the
/// server. Cap'n Proto requires sequential ordinals so this placeholder cannot
/// be removed, but the server intentionally rejects it.
#[tokio::test]
#[serial_test::parallel]
async fn make_mining_old2_rejected() {
    with_init_client(|client| async move {
        let result = client.make_mining_old2_request().send().promise.await;
        assert!(
            result.is_err(),
            "makeMiningOld2 should be rejected by the server"
        );
    })
    .await;
}

/// Check the four mining constants from the capnp schema.
#[test]
#[serial_test::parallel]
fn mining_constants() {
    assert_eq!(mining_capnp::MAX_MONEY, 2_100_000_000_000_000i64);
    const { assert!(mining_capnp::MAX_DOUBLE > 1e300) };
    assert_eq!(mining_capnp::DEFAULT_BLOCK_RESERVED_WEIGHT, 8_000u32);
    assert_eq!(
        mining_capnp::DEFAULT_COINBASE_OUTPUT_MAX_ADDITIONAL_SIGOPS,
        400u32
    );
}

/// isTestChain, isInitialBlockDownload, getTip.
#[tokio::test]
#[serial_test::parallel]
async fn mining_basic_queries() {
    with_mining_client(|_client, mining| async move {
        // isTestChain
        let resp = mining.is_test_chain_request().send().promise.await.unwrap();
        assert!(resp.get().unwrap().get_result(), "regtest is a test chain");

        // isInitialBlockDownload
        let resp = mining
            .is_initial_block_download_request()
            .send()
            .promise
            .await
            .unwrap();
        let _ibd = resp
            .get()
            .expect("isInitialBlockDownload response should decode")
            .get_result();

        // getTip
        let resp = mining.get_tip_request().send().promise.await.unwrap();
        let results = resp.get().unwrap();
        assert!(results.get_has_result(), "node should have a tip");
        let tip = results.get_result().unwrap();
        let tip_hash = tip.get_hash().unwrap();
        assert_eq!(tip_hash.len(), 32, "block hash must be 32 bytes");
        assert!(tip.get_height() >= 0, "height must be non-negative");
    })
    .await;
}

/// waitTipChanged with a short timeout.
#[tokio::test]
// Serialized because this assertion is sensitive to concurrent tip changes.
#[serial_test::serial]
async fn mining_wait_tip_changed() {
    with_mining_client(|_client, mining| async move {
        // Get the current tip first.
        let resp = mining.get_tip_request().send().promise.await.unwrap();
        let results = resp.get().unwrap();
        let tip = results.get_result().unwrap();
        let tip_hash: Vec<u8> = tip.get_hash().unwrap().to_vec();
        let tip_height: i32 = tip.get_height();

        // Wait with a short timeout; no new block should arrive.
        let mut req = mining.wait_tip_changed_request();
        req.get().set_current_tip(&tip_hash);
        req.get().set_timeout(500.0); // 500 ms
        let resp = req.send().promise.await.unwrap();
        let wait_result = resp.get().unwrap().get_result().unwrap();
        assert_eq!(wait_result.get_hash().unwrap().len(), 32);
        assert_eq!(wait_result.get_height(), tip_height);

        // No timeout. Pin a local task on the wait, then interrupt it.
        let mut req = mining.wait_tip_changed_request();
        req.get().set_current_tip(&tip_hash);
        let waiter = tokio::task::spawn_local(req.send().promise);

        // Give the waiter time to block before interrupting.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiter.is_finished(),
            "waitTipChanged returned before interrupt"
        );
        mining
            .interrupt_request()
            .send()
            .promise
            .await
            .expect("interrupt should not fail");

        let joined = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .expect("interrupt should unblock waitTipChanged");
        assert!(
            joined.is_ok_and(|resp| resp.is_ok()),
            "interrupted waitTipChanged should return normally"
        );
    })
    .await;
}

/// createNewBlock + all BlockTemplate read methods: getBlockHeader, getBlock,
/// getTxFees, getTxSigops, getCoinbaseTx, getCoinbaseMerklePath.
#[tokio::test]
#[serial_test::parallel]
async fn mining_block_template_inspection() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        // getBlockHeader
        let resp = template
            .get_block_header_request()
            .send()
            .promise
            .await
            .unwrap();
        let header = resp.get().unwrap().get_result().unwrap();
        assert_eq!(header.len(), 80, "block header must be 80 bytes");

        // getBlock
        let resp = template.get_block_request().send().promise.await.unwrap();
        let block = resp.get().unwrap().get_result().unwrap();
        assert!(block.len() > 80, "serialized block must be > 80 bytes");

        // getTxFees
        let resp = template.get_tx_fees_request().send().promise.await.unwrap();
        let _fees = resp
            .get()
            .expect("getTxFees response should decode")
            .get_result()
            .expect("getTxFees response should contain fees");

        // getTxSigops
        let resp = template
            .get_tx_sigops_request()
            .send()
            .promise
            .await
            .unwrap();
        let _sigops = resp
            .get()
            .expect("getTxSigops response should decode")
            .get_result()
            .expect("getTxSigops response should contain sigops");

        // getCoinbaseTx — inspect every CoinbaseTx field
        let resp = template
            .get_coinbase_tx_request()
            .send()
            .promise
            .await
            .unwrap();
        let coinbase = resp.get().unwrap().get_result().unwrap();
        let _version: u32 = coinbase.get_version();
        let _sequence: u32 = coinbase.get_sequence();
        let script_sig_prefix = coinbase.get_script_sig_prefix().unwrap();
        assert!(
            !script_sig_prefix.is_empty(),
            "scriptSigPrefix must contain at least the block height"
        );
        let _witness = coinbase
            .get_witness()
            .expect("coinbase witness should decode");
        let reward: i64 = coinbase.get_block_reward_remaining();
        assert!(reward > 0 && reward <= mining_capnp::MAX_MONEY);
        let _required_outputs = coinbase
            .get_required_outputs()
            .expect("coinbase required outputs should decode");
        let _lock_time: u32 = coinbase.get_lock_time();

        // getCoinbaseMerklePath
        let resp = template
            .get_coinbase_merkle_path_request()
            .send()
            .promise
            .await
            .unwrap();
        let _merkle_path = resp
            .get()
            .expect("getCoinbaseMerklePath response should decode")
            .get_result()
            .expect("getCoinbaseMerklePath response should contain a merkle path");

        destroy_template(&template).await;
    })
    .await;
}

/// waitNext (short timeout), waitNext interrupted by interruptWait, submitSolution (garbage), destroy.
#[tokio::test]
// Serialized because submitSolution behavior depends on current chain tip.
#[serial_test::serial]
async fn mining_block_template_lifecycle() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        // waitNext — short timeout, no new transactions expected.
        let mut req = template.wait_next_request();
        {
            let mut opts = req.get().init_options();
            opts.set_timeout(100.0); // 100 ms
            opts.set_fee_threshold(mining_capnp::MAX_MONEY);
        }
        let resp = req.send().promise.await.unwrap();
        let results = resp.get().expect("waitNext response should decode");
        assert!(
            !results.has_result(),
            "waitNext should time out without a new template"
        );

        // waitNext — no timeout. Pin a local task on the wait, then interrupt it.
        let mut req = template.wait_next_request();
        req.get()
            .init_options()
            .set_fee_threshold(mining_capnp::MAX_MONEY);
        let waiter = tokio::task::spawn_local(req.send().promise);

        // Give the waiter time to block before interrupting.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiter.is_finished(),
            "waitNext returned before interruptWait"
        );
        template
            .interrupt_wait_request()
            .send()
            .promise
            .await
            .expect("interruptWait should not fail");

        let joined = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .expect("interrupWait should unblock waitNext");
        assert!(
            joined.is_ok_and(|resp| resp.is_ok()),
            "interrupted waitNext should return normally"
        );

        // submitSolution — garbage coinbase should be rejected.
        // This mutates the template, so we do it right before destroy.
        let mut req = template.submit_solution_request();
        req.get().set_version(1);
        req.get().set_timestamp(0);
        req.get().set_nonce(0);
        req.get().set_coinbase(&[0u8; 64]);
        let resp = req.send().promise.await.unwrap();
        let submitted = resp.get().unwrap().get_result();
        assert!(!submitted, "garbage solution must not be accepted");

        destroy_template(&template).await;
    })
    .await;
}

/// submitSolution with insufficient PoW should return reason/debug details.
#[tokio::test]
#[serial_test::serial]
async fn mining_block_template_submit_solution_insufficient_pow() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;
        let block = block_with_pow(&block, false);
        let solution = block_solution(&block);

        let outcome = submit_solution(&template, &solution).await;
        assert!(
            !outcome.accepted,
            "solution with insufficient PoW must not be accepted"
        );
        assert_eq!(outcome.reason, "high-hash");
        assert_eq!(outcome.debug, "proof of work failed");

        destroy_template(&template).await;
    })
    .await;
}

/// submitSolution with a solved template block should be accepted.
#[tokio::test]
#[serial_test::serial]
async fn mining_block_template_submit_solution_resolved_and_duplicate() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;
        let block = block_with_pow(&block, true);
        let solution = block_solution(&block);

        let outcome = submit_solution(&template, &solution).await;
        assert!(
            outcome.accepted,
            "solved template solution must be accepted: reason={}, debug={}",
            outcome.reason, outcome.debug
        );
        assert_eq!(outcome.reason, "");
        assert_eq!(outcome.debug, "");

        let outcome = submit_solution(&template, &solution).await;
        assert!(!outcome.accepted, "duplicate solution must not be accepted");
        assert_eq!(outcome.reason, "duplicate");
        assert_eq!(outcome.debug, "");

        destroy_template(&template).await;
    })
    .await;
}

/// submitBlock with insufficient PoW should be rejected.
#[tokio::test]
#[serial_test::serial]
async fn mining_submit_block_insufficient_pow() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;
        let block = block_with_pow(&block, false);

        let outcome = submit_block(&mining, &block).await;
        assert!(
            !outcome.accepted,
            "block with insufficient PoW must not be accepted"
        );
        assert_eq!(outcome.reason, "high-hash");
        assert_eq!(outcome.debug, "proof of work failed");

        destroy_template(&template).await;
    })
    .await;
}

/// submitBlock with invalid contents should be rejected even with sufficient PoW.
#[tokio::test]
#[serial_test::serial]
async fn mining_submit_block_invalid() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;
        let mut block = block_with_pow(&block, true);
        // Corrupt the serialized block after solving its header. This keeps
        // the PoW valid while making the header's Merkle root stale.
        *block
            .last_mut()
            .expect("serialized block must not be empty") ^= 1;

        let outcome = submit_block(&mining, &block).await;
        assert!(
            !outcome.accepted,
            "invalid block with sufficient PoW must not be accepted"
        );
        assert_eq!(outcome.reason, "bad-txnmrklroot");
        assert_eq!(outcome.debug, "hashMerkleRoot mismatch");

        destroy_template(&template).await;
    })
    .await;
}

/// submitBlock with a solved template block should be accepted.
#[tokio::test]
#[serial_test::serial]
async fn mining_submit_block_resolved() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;
        let block = block_with_pow(&block, true);

        let outcome = submit_block(&mining, &block).await;
        assert!(
            outcome.accepted,
            "solved template block must be accepted: reason={}, debug={}",
            outcome.reason, outcome.debug
        );
        assert_eq!(outcome.reason, "");
        assert_eq!(outcome.debug, "");

        destroy_template(&template).await;
    })
    .await;
}

/// submitBlock with a duplicate solved block should be rejected.
#[tokio::test]
#[serial_test::serial]
async fn mining_submit_block_duplicate() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;
        let block = block_with_pow(&block, true);

        let outcome = submit_block(&mining, &block).await;
        assert!(
            outcome.accepted,
            "first solved block submission must be accepted: reason={}, debug={}",
            outcome.reason, outcome.debug
        );
        assert_eq!(outcome.reason, "");
        assert_eq!(outcome.debug, "");

        let outcome = submit_block(&mining, &block).await;
        assert!(!outcome.accepted, "duplicate block must not be accepted");
        assert_eq!(outcome.reason, "duplicate");
        assert_eq!(outcome.debug, "");

        destroy_template(&template).await;
    })
    .await;
}

/// getTransactionsByTxID and getTransactionsByWitnessID with empty lists and
/// with a non-existent txid/wtxid.
#[tokio::test]
// Serialized because this test may mine blocks to recover wallet funding.
#[serial_test::serial]
async fn mining_get_transactions() {
    with_mining_client(|_client, mining| async move {
        let wallet = bitcoin_test_wallet();
        ensure_wallet_loaded_and_funded(&wallet);

        let real_tx = create_mempool_self_transfer(&wallet);
        let real_txid = real_tx.compute_txid().to_byte_array();
        let real_wtxid = real_tx.compute_wtxid().to_byte_array();
        let real_raw_tx = encode_to_vec(&real_tx);

        // getTransactionsByTxID — empty list should return empty list.
        let mut req = mining.get_transactions_by_tx_i_d_request();
        req.get().init_txids(0);
        let resp = req.send().promise.await.unwrap();
        let results = resp.get().unwrap().get_result().unwrap();
        assert_eq!(
            results.len(),
            0,
            "empty txid list should return empty results"
        );

        // getTransactionsByTxID — return real mempool tx and empty for unknown id.
        let fake_txid = [0x42u8; 32];
        let mut req = mining.get_transactions_by_tx_i_d_request();
        {
            let mut txids = req.get().init_txids(2);
            txids.set(0, &real_txid);
            txids.set(1, &fake_txid);
        }
        let resp = req.send().promise.await.unwrap();
        let results = resp.get().unwrap().get_result().unwrap();
        assert_eq!(
            results.len(),
            2,
            "should return one entry per requested txid, including misses"
        );
        assert_eq!(
            results.get(0).unwrap(),
            real_raw_tx.as_slice(),
            "known txid should return serialized transaction"
        );
        assert!(
            results.get(1).unwrap().is_empty(),
            "non-existent txid should return empty data"
        );

        // getTransactionsByWitnessID — empty list should return empty list.
        let mut req = mining.get_transactions_by_witness_i_d_request();
        req.get().init_wtxids(0);
        let resp = req.send().promise.await.unwrap();
        let results = resp.get().unwrap().get_result().unwrap();
        assert_eq!(
            results.len(),
            0,
            "empty wtxid list should return empty results"
        );

        // getTransactionsByWitnessID — return real mempool tx and empty for unknown id.
        let fake_wtxid = [0x43u8; 32];
        let mut req = mining.get_transactions_by_witness_i_d_request();
        {
            let mut wtxids = req.get().init_wtxids(2);
            wtxids.set(0, &real_wtxid);
            wtxids.set(1, &fake_wtxid);
        }
        let resp = req.send().promise.await.unwrap();
        let results = resp.get().unwrap().get_result().unwrap();
        assert_eq!(
            results.len(),
            2,
            "should return one entry per requested wtxid, including misses"
        );
        assert_eq!(
            results.get(0).unwrap(),
            real_raw_tx.as_slice(),
            "known wtxid should return serialized transaction"
        );
        assert!(
            results.get(1).unwrap().is_empty(),
            "non-existent wtxid should return empty data"
        );
    })
    .await;
}

/// checkBlock with a template block payload.
#[tokio::test]
#[serial_test::parallel]
async fn mining_check_block() {
    with_mining_client(|_client, mining| async move {
        let template = make_block_template(&mining).await;

        let block = get_template_block(&template).await;

        // checkBlock should either error or return a response.
        let mut req = mining.check_block_request();
        req.get().set_block(&block);
        {
            let mut opts = req.get().init_options();
            opts.set_check_merkle_root(true);
            opts.set_check_pow(false);
        }
        let result = req.send().promise.await;
        match result {
            Ok(resp) => {
                let results = resp.get().expect("checkBlock response should decode");
                let _valid = results.get_result();
                let _reason = results
                    .get_reason()
                    .expect("checkBlock response should contain reason");
                let _debug = results
                    .get_debug()
                    .expect("checkBlock response should contain debug");
            }
            Err(_) => {
                // Server may reject validation/deserialization.
            }
        }

        destroy_template(&template).await;
    })
    .await;
}

/// Exercise the `context.thread` dispatch path. Clients may want to use this if they know a call
/// will block for a long time, potentially indefinitely.
#[tokio::test]
#[serial_test::parallel]
async fn echo_with_explicit_thread() {
    let rpc_network = connect_unix_stream(unix_socket_path()).await;
    let mut rpc_system = RpcSystem::new(Box::new(rpc_network), None);
    LocalSet::new()
        .run_until(async move {
            let client: init::Client = rpc_system.bootstrap(Side::Server);
            tokio::task::spawn_local(rpc_system);

            let construct_resp = client
                .construct_request()
                .send()
                .promise
                .await
                .expect("could not create initial request");
            let thread_map: thread_map::Client =
                construct_resp.get().unwrap().get_thread_map().unwrap();
            let thread_resp = thread_map
                .make_thread_request()
                .send()
                .promise
                .await
                .unwrap();
            let thread: thread::Client = thread_resp.get().unwrap().get_result().unwrap();

            let mut make_echo = client.make_echo_request();
            make_echo
                .get()
                .get_context()
                .unwrap()
                .set_thread(thread.clone());
            let echo_resp = make_echo.send().promise.await.unwrap();
            let echo = echo_resp.get().unwrap().get_result().unwrap();

            let mut req = echo.echo_request();
            req.get().get_context().unwrap().set_thread(thread.clone());
            req.get().set_echo("thread-dispatched");
            let resp = req.send().promise.await.unwrap();
            let text = resp
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .to_string()
                .unwrap();
            assert_eq!("thread-dispatched", text);
        })
        .await;
}

/// Minimal coverage for wallet/mempool helpers added for future mempool tests.
#[tokio::test]
#[serial_test::serial]
async fn wallet_helpers_create_mempool_transaction() {
    let wallet = bitcoin_test_wallet();
    assert!(!wallet.is_empty(), "test wallet name must not be empty");

    ensure_wallet_loaded_and_funded(&wallet);
    let before = mempool_tx_count();
    let _tx = create_mempool_self_transfer(&wallet);
    let after = mempool_tx_count();
    assert_eq!(
        after,
        before + 1,
        "self-transfer should add one mempool transaction"
    );
}

// -- Chain interface tests ---------------------------------------------------

/// Look up the active-chain block hash at `height` via the unstable
/// `getBlockHash`, so stable `Chain` tests don't depend on the method under
/// test to find their inputs.
async fn block_hash_at(chain: &unstable_chain::Client, height: i32) -> Vec<u8> {
    let mut req = chain.get_block_hash_request();
    req.get().set_height(height);
    let resp = req.send().promise.await.unwrap();
    resp.get().unwrap().get_result().unwrap().to_vec()
}

/// Verify `Chain.destroy` succeeds and doesn't affect other Chain
/// capabilities. Calling methods on the destroyed capability itself isn't
/// tested: the server resets the wrapped `interfaces::Chain` on destroy, so
/// a later call would dereference it after it's gone.
#[tokio::test]
#[serial_test::parallel]
async fn chain_destroy() {
    with_chain_client(|init, chain| async move {
        chain.destroy_request().send().promise.await.unwrap();

        // A freshly made Chain must still work.
        let unstable = make_unstable_chain(&init).await;
        let genesis_hash = block_hash_at(&unstable, 0).await;
        let chain = make_chain(&init).await;
        let mut req = chain.find_block_request();
        req.get().set_hash(&genesis_hash);
        req.get().init_block().set_want_height(true);
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "new Chain must find the genesis block");
        assert_eq!(result.get_block().unwrap().get_height(), 0);
    })
    .await;
}

/// Verify findBlock(wantData=true) returns the full serialized block. This is
/// the call electrs uses to fetch raw blocks via IPC instead of P2P.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_block_returns_data() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let genesis_hash = block_hash_at(&unstable, 0).await;

        let mut req = chain.find_block_request();
        req.get().set_hash(&genesis_hash);
        {
            let mut params = req.get().init_block();
            params.set_want_data(true);
            params.set_want_height(true);
            params.set_want_hash(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "genesis block must be findable");
        let block_result = result.get_block().unwrap();
        let data = block_result.get_data().unwrap();
        assert!(
            data.len() > 80,
            "serialized block (header + txs) must be > 80 bytes, got {}",
            data.len()
        );
        let echoed_hash = block_result.get_hash().unwrap();
        assert_eq!(echoed_hash, genesis_hash.as_slice());
        assert_eq!(block_result.get_height(), 0);
    })
    .await;
}

/// Parse a serialized `CBlockLocator` (int32 LE version + CompactSize count +
/// 32-byte hashes, tip first) into its list of hashes.
fn locator_hashes(blob: &[u8]) -> Vec<&[u8]> {
    assert_eq!(
        i32::from_le_bytes(blob[0..4].try_into().unwrap()),
        70016,
        "CBlockLocator starts with DUMMY_VERSION"
    );
    let count = blob[4] as usize;
    assert!(count < 0xfd, "locator count must fit a one-byte CompactSize");
    let hashes: Vec<&[u8]> = blob[5..].chunks(32).collect();
    assert_eq!(hashes.len(), count);
    assert!(hashes.iter().all(|h| h.len() == 32));
    hashes
}

/// Verify findBlock fills every FoundBlockParam field consistently for a
/// block on the active chain: hash and time agree with the decoded block
/// data, the times are ordered, and the locator starts at the block and ends
/// at genesis. nextBlock isn't requested, so no nested result comes back.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_block_all_fields() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let genesis_hash = block_hash_at(&unstable, 0).await;
        let hash_99 = block_hash_at(&unstable, 99).await;
        let hash_100 = block_hash_at(&unstable, 100).await;

        let mut req = chain.find_block_request();
        req.get().set_hash(&hash_100);
        {
            let mut params = req.get().init_block();
            params.set_want_hash(true);
            params.set_want_height(true);
            params.set_want_time(true);
            params.set_want_max_time(true);
            params.set_want_mtp_time(true);
            params.set_want_in_active_chain(true);
            params.set_want_locator(true);
            params.set_want_data(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "block 100 must be found");
        let block = result.get_block().unwrap();
        assert!(block.get_found());

        assert_eq!(block.get_hash().unwrap(), hash_100.as_slice());
        assert_eq!(block.get_height(), 100);
        assert!(block.get_in_active_chain(), "block 100 is on the active chain");

        // The block data must hash to the returned hash, link to block 99 and
        // carry the returned timestamp.
        let data = block.get_data().unwrap();
        let header: bitcoin::BlockHeader = encoding::decode_from_slice(&data[..80])
            .unwrap_or_else(|e| panic!("failed to decode block header: {e}"));
        let _: bitcoin::Block = encoding::decode_from_slice(data)
            .unwrap_or_else(|e| panic!("failed to decode block: {e}"));
        assert_eq!(header.block_hash().as_byte_array(), hash_100.as_slice());
        assert_eq!(header.prev_blockhash.as_byte_array(), hash_99.as_slice());
        assert_eq!(block.get_time(), header.time.to_u32() as i64);

        // maxTime is the max timestamp up to this block, and MTP is the median
        // of the previous 11, so neither can be out of order with time.
        assert!(block.get_max_time() >= block.get_time());
        assert!(block.get_mtp_time() > 0);
        assert!(block.get_mtp_time() <= block.get_max_time());

        let locator = locator_hashes(block.get_locator().unwrap());
        assert_eq!(locator.first().copied(), Some(hash_100.as_slice()));
        assert_eq!(locator.last().copied(), Some(genesis_hash.as_slice()));

        assert!(!block.has_next_block(), "nextBlock was not requested");
    })
    .await;
}

/// Verify findBlock with an empty FoundBlockParam reports the block as found
/// but fills in nothing else: fields are only set when requested.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_block_unrequested_fields_left_empty() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let hash_100 = block_hash_at(&unstable, 100).await;

        let mut req = chain.find_block_request();
        req.get().set_hash(&hash_100);
        req.get().init_block();
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "block 100 must be found");
        let block = result.get_block().unwrap();
        assert!(block.get_found());

        assert!(block.get_hash().unwrap().is_empty());
        assert_eq!(block.get_height(), 0);
        assert_eq!(block.get_time(), 0);
        assert_eq!(block.get_max_time(), 0);
        assert_eq!(block.get_mtp_time(), 0);
        assert!(!block.get_in_active_chain());
        assert!(block.get_locator().unwrap().is_empty());
        assert!(block.get_data().unwrap().is_empty());
        assert!(!block.has_next_block());
    })
    .await;
}

/// Verify nextBlock follows the active chain through several nested levels,
/// and that each level honors only its own want flags.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_block_next_block_chain() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let hash_50 = block_hash_at(&unstable, 50).await;
        let hash_51 = block_hash_at(&unstable, 51).await;
        let hash_52 = block_hash_at(&unstable, 52).await;
        let hash_53 = block_hash_at(&unstable, 53).await;

        let mut req = chain.find_block_request();
        req.get().set_hash(&hash_50);
        {
            // Level 0 (block 50): height only.
            let mut level0 = req.get().init_block();
            level0.set_want_height(true);
            // Level 1 (block 51): hash and height.
            let mut level1 = level0.init_next_block();
            level1.set_want_hash(true);
            level1.set_want_height(true);
            // Level 2 (block 52): hash, height and active-chain flag.
            let mut level2 = level1.init_next_block();
            level2.set_want_hash(true);
            level2.set_want_height(true);
            level2.set_want_in_active_chain(true);
            // Level 3 (block 53): hash only.
            level2.init_next_block().set_want_hash(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "block 50 must be found");

        let level0 = result.get_block().unwrap();
        assert!(level0.get_found());
        assert_eq!(level0.get_height(), 50);
        assert!(level0.get_hash().unwrap().is_empty(), "level 0 didn't ask for its hash");

        let level1 = level0.get_next_block().unwrap();
        assert!(level1.get_found());
        assert_eq!(level1.get_height(), 51);
        assert_eq!(level1.get_hash().unwrap(), hash_51.as_slice());

        let level2 = level1.get_next_block().unwrap();
        assert!(level2.get_found());
        assert_eq!(level2.get_height(), 52);
        assert_eq!(level2.get_hash().unwrap(), hash_52.as_slice());
        assert!(level2.get_in_active_chain());

        let level3 = level2.get_next_block().unwrap();
        assert!(level3.get_found());
        assert_eq!(level3.get_hash().unwrap(), hash_53.as_slice());
        assert_eq!(level3.get_height(), 0, "level 3 didn't ask for its height");
        assert!(!level3.has_next_block(), "level 3 didn't ask for a next block");
    })
    .await;
}

/// Verify nextBlock on the tip: the tip itself is found, but it has no next
/// block, so the nested result reports not found. Serialized so no other
/// test moves the tip while it runs.
#[tokio::test]
#[serial_test::serial]
async fn chain_find_block_next_block_at_tip() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let resp = unstable.get_height_request().send().promise.await.unwrap();
        let tip_height: i32 = resp.get().unwrap().get_result();
        let tip_hash = block_hash_at(&unstable, tip_height).await;

        let mut req = chain.find_block_request();
        req.get().set_hash(&tip_hash);
        {
            let mut params = req.get().init_block();
            params.set_want_height(true);
            let mut next = params.init_next_block();
            next.set_want_hash(true);
            next.set_want_height(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "the tip must be found");
        let block = result.get_block().unwrap();
        assert!(block.get_found());
        assert_eq!(block.get_height(), tip_height);

        let next = block.get_next_block().unwrap();
        assert!(!next.get_found(), "the tip has no next block");
        assert!(next.get_hash().unwrap().is_empty());
        assert_eq!(next.get_height(), 0);
    })
    .await;
}

/// Call a JSON-RPC method through the IPC `Rpc` interface and return its
/// `result`, panicking if the node reports an error.
async fn rpc_call(rpc: &rpc::Client, method: &str, params: Value) -> Value {
    let mut req = rpc.execute_rpc_request();
    let body = json!({ "jsonrpc": "2.0", "id": "test", "method": method, "params": params });
    req.get().set_request(body.to_string());
    let resp = req.send().promise.await.unwrap();
    let text = resp.get().unwrap().get_result().unwrap().to_string().unwrap();
    let v: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("failed to parse {method} response as JSON: {e}"));
    assert!(v["error"].is_null(), "{method} failed: {}", v["error"]);
    v["result"].clone()
}

/// Verify nextBlock on a stale block (valid, but not on the active chain)
/// reports not found, since the node only follows nextBlock along the active
/// chain. Also checks that nextBlock from the fork point follows the active
/// branch, not the stale one.
///
/// Builds the fork over the IPC `Rpc` interface by invalidating the tip,
/// mining two replacement blocks and reconsidering the old tip. Two blocks
/// give the new branch more work, so the reconsidered block stays stale
/// instead of winning a tie. Serialized because it reorgs the shared node.
#[tokio::test]
#[serial_test::serial]
async fn chain_find_block_next_block_on_stale_block() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let rpc = make_rpc(&init).await;
        let resp = unstable.get_height_request().send().promise.await.unwrap();
        let fork_height: i32 = resp.get().unwrap().get_result();
        let fork_parent_hash = block_hash_at(&unstable, fork_height - 1).await;
        let stale_hash = block_hash_at(&unstable, fork_height).await;
        let stale_hex = rpc_call(&rpc, "getblockhash", json!([fork_height])).await;

        rpc_call(&rpc, "invalidateblock", json!([stale_hex])).await;
        // raw(51) is an OP_TRUE output script, so no wallet address is needed.
        rpc_call(&rpc, "generatetodescriptor", json!([2, "raw(51)"])).await;
        rpc_call(&rpc, "reconsiderblock", json!([stale_hex])).await;

        let active_hash = block_hash_at(&unstable, fork_height).await;
        assert_ne!(active_hash, stale_hash, "the old tip must have been replaced");

        // The stale block is still found, but has no next block.
        let mut req = chain.find_block_request();
        req.get().set_hash(&stale_hash);
        {
            let mut params = req.get().init_block();
            params.set_want_height(true);
            params.set_want_in_active_chain(true);
            let mut next = params.init_next_block();
            next.set_want_hash(true);
            next.set_want_height(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "a stale block is still known to the node");
        let block = result.get_block().unwrap();
        assert!(block.get_found());
        assert_eq!(block.get_height(), fork_height);
        assert!(!block.get_in_active_chain(), "the reconsidered block must be stale");
        let next = block.get_next_block().unwrap();
        assert!(!next.get_found(), "a stale block has no next block");
        assert!(next.get_hash().unwrap().is_empty());
        assert_eq!(next.get_height(), 0);

        // From the fork point, nextBlock follows the active branch.
        let mut req = chain.find_block_request();
        req.get().set_hash(&fork_parent_hash);
        {
            let mut next = req.get().init_block().init_next_block();
            next.set_want_hash(true);
            next.set_want_in_active_chain(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "the fork point must be found");
        let next = result.get_block().unwrap().get_next_block().unwrap();
        assert!(next.get_found());
        assert_eq!(next.get_hash().unwrap(), active_hash.as_slice());
        assert!(next.get_in_active_chain());
    })
    .await;
}

/// Verify findBlock on an unknown hash returns false and leaves both the
/// block and its requested nextBlock unfound.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_block_unknown_hash() {
    with_chain_client(|_init, chain| async move {
        let mut req = chain.find_block_request();
        req.get().set_hash(&[0u8; 32]);
        {
            let mut params = req.get().init_block();
            params.set_want_hash(true);
            params.set_want_height(true);
            params.init_next_block().set_want_hash(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(!result.get_result(), "an all-zero hash is not a known block");
        let block = result.get_block().unwrap();
        assert!(!block.get_found());
        assert!(block.get_hash().unwrap().is_empty());
        assert!(!block.get_next_block().unwrap().get_found());
    })
    .await;
}

/// Verify findAncestorByHeight returns the expected block hash.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_ancestor_by_height() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let hash_50 = block_hash_at(&unstable, 50).await;
        let hash_100 = block_hash_at(&unstable, 100).await;

        // Ask for the ancestor at height 50 starting from block 100.
        let mut req = chain.find_ancestor_by_height_request();
        req.get().set_block_hash(&hash_100);
        req.get().set_ancestor_height(50);
        {
            let mut params = req.get().init_ancestor();
            params.set_want_hash(true);
            params.set_want_height(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "ancestor at height 50 must exist");
        let ancestor = result.get_ancestor().unwrap();
        assert_eq!(ancestor.get_height(), 50);
        assert_eq!(ancestor.get_hash().unwrap(), hash_50.as_slice());
    })
    .await;
}

/// Verify findFirstBlockWithTimeAndHeight returns the first active-chain
/// block at or above both thresholds, and reports not-found when no block is
/// late enough. This is how the wallet locates the rescan start block for a
/// key birth time.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_first_block_with_time_and_height() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let expected_hash = block_hash_at(&unstable, 10).await;

        // Every block satisfies minTime = 0, so the height bound decides.
        let mut req = chain.find_first_block_with_time_and_height_request();
        req.get().set_min_time(0);
        req.get().set_min_height(10);
        {
            let mut params = req.get().init_block();
            params.set_want_hash(true);
            params.set_want_height(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "block at height 10 must be found");
        let block = result.get_block().unwrap();
        assert_eq!(block.get_height(), 10);
        assert_eq!(block.get_hash().unwrap(), expected_hash.as_slice());

        // No regtest block is timestamped in 2106.
        let mut req = chain.find_first_block_with_time_and_height_request();
        req.get().set_min_time(u32::MAX as i64);
        req.get().set_min_height(0);
        req.get().init_block().set_want_height(true);
        let resp = req.send().promise.await.unwrap();
        assert!(
            !resp.get().unwrap().get_result(),
            "no block should satisfy a far-future minTime"
        );
    })
    .await;
}

/// Verify findAncestorByHash confirms a real ancestor and rejects a block
/// that is a descendant rather than an ancestor.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_ancestor_by_hash() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let hash_50 = block_hash_at(&unstable, 50).await;
        let hash_100 = block_hash_at(&unstable, 100).await;

        let mut req = chain.find_ancestor_by_hash_request();
        req.get().set_block_hash(&hash_100);
        req.get().set_ancestor_hash(&hash_50);
        {
            let mut params = req.get().init_ancestor();
            params.set_want_hash(true);
            params.set_want_height(true);
        }
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "block 50 must be an ancestor of block 100");
        let ancestor = result.get_ancestor().unwrap();
        assert_eq!(ancestor.get_height(), 50);
        assert_eq!(ancestor.get_hash().unwrap(), hash_50.as_slice());

        // Reversed: block 100 is not an ancestor of block 50.
        let mut req = chain.find_ancestor_by_hash_request();
        req.get().set_block_hash(&hash_50);
        req.get().set_ancestor_hash(&hash_100);
        req.get().init_ancestor().set_want_height(true);
        let resp = req.send().promise.await.unwrap();
        assert!(
            !resp.get().unwrap().get_result(),
            "block 100 must not be an ancestor of block 50"
        );
    })
    .await;
}

/// Verify findCommonAncestor on two blocks of the active chain returns the
/// lower one as the common ancestor and fills in both input blocks.
#[tokio::test]
#[serial_test::parallel]
async fn chain_find_common_ancestor() {
    with_chain_client(|init, chain| async move {
        let unstable = make_unstable_chain(&init).await;
        let hash_50 = block_hash_at(&unstable, 50).await;
        let hash_100 = block_hash_at(&unstable, 100).await;

        let mut req = chain.find_common_ancestor_request();
        req.get().set_block_hash1(&hash_50);
        req.get().set_block_hash2(&hash_100);
        {
            let mut params = req.get().init_ancestor();
            params.set_want_hash(true);
            params.set_want_height(true);
        }
        req.get().init_block1().set_want_height(true);
        req.get().init_block2().set_want_height(true);
        let resp = req.send().promise.await.unwrap();
        let result = resp.get().unwrap();
        assert!(result.get_result(), "blocks on the active chain share an ancestor");
        let ancestor = result.get_ancestor().unwrap();
        assert_eq!(ancestor.get_height(), 50);
        assert_eq!(ancestor.get_hash().unwrap(), hash_50.as_slice());
        assert_eq!(result.get_block1().unwrap().get_height(), 50);
        assert_eq!(result.get_block2().unwrap().get_height(), 100);
    })
    .await;
}

// -- UnstableChain interface tests ---------------------------------------------------

/// Smoke test the Chain interface bootstrap path and basic queries.
#[tokio::test]
#[serial_test::parallel]
async fn unstable_chain_basic_queries() {
    with_unstable_chain_client(|_init, chain| async move {
        // getHeight
        let resp = chain.get_height_request().send().promise.await.unwrap();
        let res = resp.get().unwrap();
        assert!(res.get_has_result(), "node should have a tip height");
        let height: i32 = res.get_result();
        assert!(
            height >= 101,
            "bootstrap helper should ensure height >= 101"
        );

        // getBlockHash at the tip
        let mut req = chain.get_block_hash_request();
        req.get().set_height(height);
        let resp = req.send().promise.await.unwrap();
        let hash = resp.get().unwrap().get_result().unwrap();
        assert_eq!(hash.len(), 32, "block hash must be 32 bytes");

        // isInitialBlockDownload
        let resp = chain.is_initial_block_download_request().send().promise.await.unwrap();
        let _ibd: bool = resp.get().unwrap().get_result();

        // havePruned
        let resp = chain.have_pruned_request().send().promise.await.unwrap();
        assert!(!resp.get().unwrap().get_result(), "regtest is not pruned");
    })
    .await;
}

/// Verify broadcastTransaction succeeds for a transaction that is already in
/// the mempool. The node treats this as a re-announcement and returns OK,
/// which is enough to exercise the request/response framing end-to-end.
///
/// This is the call electrs uses (when configured with the IPC backend) to
/// replace JSON-RPC `sendrawtransaction`.
#[tokio::test]
#[serial_test::serial]
async fn unstable_chain_broadcast_transaction() {
    let wallet = bitcoin_test_wallet();
    ensure_wallet_loaded_and_funded(&wallet);
    let tx = create_mempool_self_transfer(&wallet);
    let tx_bytes = encoding::encode_to_vec(&tx);

    with_unstable_chain_client(|_init, chain| async move {
        let mut req = chain.broadcast_transaction_request();
        req.get().set_tx(&tx_bytes);
        // max_tx_fee = 0 means "don't enforce a maximum"; the node will skip
        // the test_accept fee check and re-announce the existing mempool tx.
        req.get().set_max_tx_fee(0);
        // max_tx_fee_rate is a serialized CFeeRate (int64 LE fee + int32 LE
        // size); a zero feerate likewise disables the feerate check.
        req.get().set_max_tx_fee_rate(&[0u8; 12]);
        // node::TxBroadcast::MEMPOOL_AND_BROADCAST_TO_ALL = 0
        req.get().set_broadcast_method(0);
        let resp = req.send().promise.await.unwrap();
        let r = resp.get().unwrap();
        assert!(
            r.get_result(),
            "broadcastTransaction should succeed (re-announce path); error: {:?}",
            r.get_error().ok().and_then(|e| e.to_str().ok())
        );
    })
    .await;
}

/// Verify findLocatorFork returns the height of the last common block when
/// passed a CBlockLocator containing only the genesis block hash.
///
/// CBlockLocator wire format (Bitcoin Core src/primitives/block.h):
///   int32 LE version (DUMMY_VERSION = 70016) + CompactSize count + 32*count
///   block hashes ordered tip→genesis.
///
/// This is the call electrs uses to find the fork point between its own
/// header chain and the node's active chain when syncing new headers via IPC
/// instead of P2P `getheaders`.
#[tokio::test]
#[serial_test::parallel]
async fn unstable_chain_find_locator_fork() {
    with_unstable_chain_client(|_init, chain| async move {
        // Get genesis hash via getBlockHash(0).
        let mut req = chain.get_block_hash_request();
        req.get().set_height(0);
        let resp = req.send().promise.await.unwrap();
        let genesis_hash: Vec<u8> = resp.get().unwrap().get_result().unwrap().to_vec();
        assert_eq!(genesis_hash.len(), 32);

        // Build CBlockLocator { version=70016, vHave=[genesis] }.
        let mut locator = Vec::with_capacity(4 + 1 + 32);
        locator.extend_from_slice(&70016i32.to_le_bytes());
        locator.push(1u8); // CompactSize for length 1
        locator.extend_from_slice(&genesis_hash);

        let mut req = chain.find_locator_fork_request();
        req.get().set_locator(&locator);
        let resp = req.send().promise.await.unwrap();
        let r = resp.get().unwrap();
        assert!(
            r.get_has_result(),
            "fork height must be reported for genesis-only locator"
        );
        assert_eq!(
            r.get_result(),
            0,
            "fork height for genesis-only locator must be 0"
        );
    })
    .await;
}

/// Verify waitForNotificationsIfTipChanged returns immediately when the
/// supplied `oldTip` does not match the current tip. This is the cheap
/// no-op case (called when electrs is already aware of the latest tip).
#[tokio::test]
#[serial_test::parallel]
async fn unstable_chain_wait_for_notifications_returns_immediately_when_tip_differs() {
    with_unstable_chain_client(|_init, chain| async move {
        let mut req = chain.wait_for_notifications_if_tip_changed_request();
        // All-zeros hash is guaranteed not to be the current tip.
        req.get().set_old_tip(&[0u8; 32]);
        let fut = req.send().promise;
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), fut).await;
        assert!(
            result.is_ok(),
            "waitForNotificationsIfTipChanged should return ~immediately when oldTip != current tip"
        );
        result.unwrap().unwrap();
    })
    .await;
}

/// Verify waitForNotificationsIfTipChanged blocks until a new block arrives
/// when called with the current tip hash, then returns. This is the call
/// electrs uses (when configured with the IPC backend) to replace its P2P
/// `inv`-watching loop for new-block notifications.
#[tokio::test]
#[serial_test::serial]
async fn unstable_chain_wait_for_notifications_unblocks_on_new_block() {
    let wallet = bitcoin_test_wallet();
    ensure_wallet_loaded_and_funded(&wallet);

    with_unstable_chain_client(|_init, chain| async move {
        // Snapshot current tip.
        let resp = chain.get_height_request().send().promise.await.unwrap();
        let height: i32 = resp.get().unwrap().get_result();

        let mut req = chain.get_block_hash_request();
        req.get().set_height(height);
        let resp = req.send().promise.await.unwrap();
        let tip_hash: Vec<u8> = resp.get().unwrap().get_result().unwrap().to_vec();

        // Kick off the wait *before* mining the block so we exercise the
        // blocking path rather than the immediate-return path.
        let mut req = chain.wait_for_notifications_if_tip_changed_request();
        req.get().set_old_tip(&tip_hash);
        let wait_fut = req.send().promise;

        // Mine one block from a separate task so we don't deadlock the
        // current_thread runtime on the bitcoin-cli subprocess.
        let mine_wallet = wallet.clone();
        let mine_handle = tokio::task::spawn_blocking(move || {
            // Small delay so the wait is established before the block lands.
            std::thread::sleep(std::time::Duration::from_millis(200));
            mine_blocks_to_new_address(&mine_wallet, 1)
                .expect("failed to mine block to wake the wait");
        });

        let result = tokio::time::timeout(std::time::Duration::from_secs(15), wait_fut).await;
        assert!(
            result.is_ok(),
            "waitForNotificationsIfTipChanged should unblock after a new block is mined"
        );
        result.unwrap().unwrap();
        mine_handle.await.unwrap();
    })
    .await;
}

/// Minimal `ChainNotifications::Server` used by the notification tests
/// below. Records every `transactionAddedToMempool` /
/// `transactionRemovedFromMempool` event in interior-mutable buffers so the
/// test body can poll them. Other notification methods accept and ignore
/// the call (they're delivered too, but the tests don't assert on them).
#[derive(Default)]
struct RecordingNotifications {
    added: Rc<RefCell<Vec<Vec<u8>>>>,
    #[allow(clippy::type_complexity)]
    removed: Rc<RefCell<Vec<(Vec<u8>, i32)>>>,
}

impl chain_notifications::Server for RecordingNotifications {
    fn destroy(
        self: Rc<Self>,
        _: chain_notifications::DestroyParams,
        _: chain_notifications::DestroyResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    fn transaction_added_to_mempool(
        self: Rc<Self>,
        params: chain_notifications::TransactionAddedToMempoolParams,
        _: chain_notifications::TransactionAddedToMempoolResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        let added = self.added.clone();
        async move {
            let p = params.get()?;
            let tx = p.get_tx()?.to_vec();
            added.borrow_mut().push(tx);
            Ok(())
        }
    }

    fn transaction_removed_from_mempool(
        self: Rc<Self>,
        params: chain_notifications::TransactionRemovedFromMempoolParams,
        _: chain_notifications::TransactionRemovedFromMempoolResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        let removed = self.removed.clone();
        async move {
            let p = params.get()?;
            let tx = p.get_tx()?.to_vec();
            let reason = p.get_reason();
            removed.borrow_mut().push((tx, reason));
            Ok(())
        }
    }

    fn block_connected(
        self: Rc<Self>,
        _: chain_notifications::BlockConnectedParams,
        _: chain_notifications::BlockConnectedResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    fn block_disconnected(
        self: Rc<Self>,
        _: chain_notifications::BlockDisconnectedParams,
        _: chain_notifications::BlockDisconnectedResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    fn updated_block_tip(
        self: Rc<Self>,
        _: chain_notifications::UpdatedBlockTipParams,
        _: chain_notifications::UpdatedBlockTipResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }

    fn chain_state_flushed(
        self: Rc<Self>,
        _: chain_notifications::ChainStateFlushedParams,
        _: chain_notifications::ChainStateFlushedResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        std::future::ready(Ok(()))
    }
}

/// Verify `Chain.handleNotifications` delivers `transactionAddedToMempool`
/// callbacks when a new transaction enters the node's mempool. This is the
/// path electrs (when configured with the IPC backend) needs to use to
/// retire its periodic `getrawmempool` polling.
#[tokio::test]
#[serial_test::serial]
async fn unstable_chain_handle_notifications_delivers_mempool_added() {
    let wallet = bitcoin_test_wallet();
    ensure_wallet_loaded_and_funded(&wallet);

    with_unstable_chain_client(|_init, chain| async move {
        let recorder = Rc::new(RecordingNotifications::default());
        let recorder_for_assert = recorder.clone();
        let notifications: chain_notifications::Client =
            capnp_rpc::new_client_from_rc(recorder.clone());

        // Register the handler. We must keep the returned Handler client
        // alive for the duration of the subscription.
        let mut req = chain.handle_notifications_request();
        req.get().set_notifications(notifications.clone());
        let resp = req.send().promise.await.unwrap();
        let _handler = resp.get().unwrap().get_result().unwrap();

        // Drain any prior mempool state so the subsequent self-transfer is
        // unambiguously the trigger.
        recorder_for_assert.added.borrow_mut().clear();

        // Inject a fresh transaction into the node's mempool from a
        // blocking task (bitcoin-cli is sync) and wait for the notification
        // to arrive.
        let inject_wallet = wallet.clone();
        let inject = tokio::task::spawn_blocking(move || {
            create_mempool_self_transfer(&inject_wallet);
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if !recorder_for_assert.added.borrow().is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "transactionAddedToMempool was not delivered within 15s; recorded={}",
                    recorder_for_assert.added.borrow().len()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        inject.await.unwrap();
    })
    .await;
}

/// Verify `Chain.requestMempoolTransactions` replays the current mempool
/// contents through the supplied `ChainNotifications` handler. This is the
/// "give me what's already in the mempool" primer electrs needs at startup
/// before switching to the live notification stream.
#[tokio::test]
#[serial_test::serial]
async fn unstable_chain_request_mempool_transactions_replays_current_mempool() {
    let wallet = bitcoin_test_wallet();
    ensure_wallet_loaded_and_funded(&wallet);

    // Seed the mempool with a single transaction synchronously (outside
    // the runtime) so it's already there when we ask for the snapshot.
    let _seed = create_mempool_self_transfer(&wallet);
    assert!(
        mempool_tx_count() >= 1,
        "expected at least one tx in the node's mempool before request"
    );

    with_unstable_chain_client(|_init, chain| async move {
        let recorder = Rc::new(RecordingNotifications::default());
        let notifications: chain_notifications::Client =
            capnp_rpc::new_client_from_rc(recorder.clone());

        let mut req = chain.request_mempool_transactions_request();
        req.get().set_notifications(notifications);
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), req.send().promise)
            .await
            .expect("requestMempoolTransactions timed out");
        resp.expect("requestMempoolTransactions failed");

        // The replay is fire-and-forget on the server side; give the
        // delivered callbacks a brief moment to land on our LocalSet
        // before asserting.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !recorder.added.borrow().is_empty() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "requestMempoolTransactions delivered no transactionAddedToMempool callbacks"
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
}
