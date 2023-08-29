open! Core
open! Async_kernel
open! Import

let debug_on_find_state = ref ignore

module Make (Key : sig
    type t [@@deriving sexp_of, hash, compare]
  end) =
struct
  module Tag = struct
    type 'job_tag t =
      | User_job of 'job_tag option
      | Prior_jobs_done
    [@@deriving sexp]
  end

  module Job = struct
    type ('state, 'job_tag) t =
      { tag : 'job_tag Tag.t
      ; run : 'state option -> unit Deferred.t
      }

    let sexp_of_t _ sexp_of_job_tag t = t.tag |> [%sexp_of: job_tag Tag.t]
  end

  type ('state, 'job_tag) t =
    { states : (Key.t, 'state) Hashtbl.t
    (* We use a [Queue.t] and implement the [Throttle.Sequencer] functionality ourselves,
       because throttles don't provide a way to get notified when they are empty, and we
       need to remove the table entry for an emptied throttle. *)
    ; jobs : (Key.t, ('state, 'job_tag) Job.t Queue.t) Hashtbl.t
    }
  [@@deriving sexp_of]

  let create () =
    { states = Hashtbl.create (module Key); jobs = Hashtbl.create (module Key) }
  ;;

  let rec run_jobs_until_none_remain t ~key (queue : (_, _) Job.t Queue.t) =
    match Queue.peek queue with
    | None -> Hashtbl.remove t.jobs key
    | Some job ->
      (* The state of [key] is found and fed to [job] immediately; there should be no
         deferred in between. *)
      let state = Hashtbl.find t.states key in
      !debug_on_find_state ();
      job.run state
      >>> fun () ->
      assert (phys_equal (Queue.dequeue_exn queue) job);
      run_jobs_until_none_remain t ~key queue
  ;;

  let set_state t ~key = function
    | None -> Hashtbl.remove t.states key
    | Some state -> Hashtbl.set t.states ~key ~data:state
  ;;

  let enqueue t ~key ?tag f =
    Deferred.create (fun ivar ->
      (* when job is called, [f] is invoked immediately, there shall be no deferred in
         between *)
      let run state_opt =
        Monitor.try_with
          ~rest:`Log
          ~run:`Now
          (fun () -> f state_opt)
        >>| Ivar.fill_exn ivar
      in
      let job = { Job.tag = Tag.User_job tag; run } in
      match Hashtbl.find t.jobs key with
      | Some queue -> Queue.enqueue queue job
      | None ->
        let queue = Queue.create () in
        Queue.enqueue queue job;
        Hashtbl.set t.jobs ~key ~data:queue;
        (* never start a job in the same async job *)
        upon Deferred.unit (fun () -> run_jobs_until_none_remain t ~key queue))
    >>| function
    | Error exn -> raise (Monitor.extract_exn exn)
    | Ok res -> res
  ;;

  let find_state t key = Hashtbl.find t.states key

  let num_unfinished_jobs t key =
    match Hashtbl.find t.jobs key with
    | None -> 0
    | Some queue -> Queue.length queue
  ;;

  let mem t key = Hashtbl.mem t.states key || Hashtbl.mem t.jobs key

  let all_keys t =
    let all_keys =
      Hash_set.create (module Key) ~size:(Hashtbl.length t.jobs + Hashtbl.length t.states)
    in
    Hashtbl.iteri t.jobs ~f:(fun ~key ~data:_ -> Hash_set.add all_keys key);
    Hashtbl.iteri t.states ~f:(fun ~key ~data:_ -> Hash_set.add all_keys key);
    all_keys
  ;;

  let fold t ~init ~f =
    Hash_set.fold (all_keys t) ~init ~f:(fun acc key ->
      f acc ~key (Hashtbl.find t.states key))
  ;;

  let exists t ~f =
    Hash_set.exists (all_keys t) ~f:(fun key -> f key (Hashtbl.find t.states key))
  ;;

  let prior_jobs_done t =
    Hashtbl.fold t.jobs ~init:[] ~f:(fun ~key:_ ~data:queue acc ->
      let this_key_done =
        Deferred.create (fun ivar ->
          Queue.enqueue
            queue
            { tag = Tag.Prior_jobs_done
            ; run =
                (fun _ ->
                   Ivar.fill_exn ivar ();
                   Deferred.unit)
            })
      in
      this_key_done :: acc)
    |> Deferred.all_unit
  ;;
end

module For_testing = struct
  let debug_on_find_state = debug_on_find_state
end
