<template>
  <div id="graph-container">

      <d3-network @node-click="goToPerson" style="height: inherit;" ref='net' :net-nodes="nodes" :net-links="[]" :options="options"/>

    <footer class="footer"></footer>
  </div>
</template>

<script>


    import D3Network from 'vue-d3-network'

    export default {
        data(){
          return{
            name: 'Graph',
            nodes: [],
            nodeSize: 23
          }
        },
        components: {
          D3Network
        },

        computed: {
            options(){
              return{
                nodeSize: this.nodeSize,
                nodeLabels: true,
                force: 1500
              }
            },

        },

        created() {
          let nodes = [];
          fetch('src/1.json').then(function (resp) {
            return resp.json();
          }).then(function (data) {
            let cus_id = data.customer_id;
            let names = data.first_name;
            let surnames = data.last_name;
            for(let i = 0; i < 60; i++){
              nodes.push({id: cus_id[i], name: names[i] + " " + surnames[i]});
            }
          });

          this.nodes = nodes;
        },

        methods: {
          goToPerson(event, node){
            this.$router.push({name: 'Person', params: {name: node.name, id: node.id}});
          }
        }

    }
</script>

<style scoped>

  #graph-container {
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #2c3e50;
    height: 100%;
  }

  div.container{
    height: 100%;
  }

  h1, h2 {
    font-weight: normal;
  }

</style>

