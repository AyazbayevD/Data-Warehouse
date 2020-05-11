<template>
  <div id="person">

    <div class="container" style="margin-top: 5%;">

      <div class="d-flex align-items-center p-3 my-3 rounded shadow-sm" style="background-color: #f1f1f1;">
        <div class="lh-100">
          <h3>{{name}}</h3>
        </div>
      </div>

      <div class="my-3 p-3 bg-white rounded shadow-sm" style="background-color: whitesmoke;">
        <div class="media text-muted pt-3">
          <p class="media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
            <strong class="d-block text-gray-dark">Номер телефона:</strong>
            87918923843
          </p>
        </div>
        <div class="media text-muted pt-3">
          <p class="media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
            <strong class="d-block text-gray-dark">Почта:</strong>
            example@gmail.com
          </p>
        </div>
      </div>

      <div class="my-3 p-3 bg-white rounded shadow-sm">
        <h6 class="border-gray pb-2 mb-0">Упоминания:</h6>
        <div class="media text-muted pt-3" v-for="i in 4">
          <p class="media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
            Источник
          </p>
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
