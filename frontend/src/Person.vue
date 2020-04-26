<template>
  <div id="person"  style="text-align: center; margin-top: 25vh">

    <div class="container">

      <div class="row">
        <div class="col-12">
          <div class="card bg-light">
            <div class="card-body">
              <h5 class="card-title">{{name}}</h5>
              <p class="card-text">
                id: {{id}} <br/>
                Some information about student
              </p>
            </div>
          </div>
        </div>
      </div>

      <div class="row">

        <div class="col-6">
          <div class="card">
            <div class="card-body">
              <h5 class="card-title">Достижения</h5>
              <p class="card-text">
                Some information about student
              </p>
            </div>
          </div>
        </div>

        <div class="col-6">
          <div class="card">
            <div class="card-body">
              <h5 class="card-title">Контакты</h5>
              <p class="card-text">
                Some information about student
              </p>
            </div>
          </div>
        </div>
      </div>

      <div class="row">
        <div class="col-12">
          <div class="card bg-light">
            <div class="card-body">
              <p class="card-text"><small class="text-muted">Last updated 3 mins ago</small></p>
            </div>
          </div>
        </div>
      </div>

    </div>
  </div>
</template>

<script>
    export default {
        data(){
          return {
            lol: 'hello'
          }
        },


        props:{
          id: String,
          name: String
        }
    }
</script>

<style scoped>

</style>
