spec:
  containers:
    - name: platoon-node
      image: ${container_image}
      env:
        - name: NODE_ID
          value: "${node_id}"
        - name: CLUSTER_SIZE
          value: "${cluster_size}"
        - name: RUST_LOG
          value: "info"
        - name: RAFT_SERVER_PORT
          value: "${raft_port}"
        - name: PLATOON_SERVER_PORT
          value: "${platoon_port}"
        - name: RND_SEED
          value: "${rnd_seed}"
        - name: READ_PERIOD
          value: "${read_period}"
        - name: WRITE_PERIOD
          value: "${write_period}"
      ports:
        - containerPort: ${raft_port}
        - containerPort: ${platoon_port}
      restartPolicy: Always
