<template>
  <div id="home">
      <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <a class="navbar-brand" href="#">Data Warehouse</a>
        <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
          <span class="navbar-toggler-icon"></span>
        </button>

        <div class="collapse navbar-collapse" id="navbarSupportedContent">
          <ul class="navbar-nav mr-auto">

            <li @click="goToPath('uploadFiles')" class="nav-item">
              <a class="nav-link" href="#">Загрузить файл<span class="sr-only">(current)</span></a>
            </li>


            <li @click="goToPath('Table')">
              <a class="nav-link" href="#">Таблица</a>
            </li>

            <li @click="goToPath('Graph')" class="nav-item">
              <a class="nav-link" href="#">Граф</a>
            </li>
          </ul>

          <form class="form-inline my-2 my-lg-0">
            <button class="btn btn-outline-success my-2 my-sm-0" type="submit">Выйти</button>
          </form>

        </div>
      </nav>

      <router-view/>
  </div>
</template>

<script>
    export default {
        data(){
          return{
            name: 'Home'
          }
        },

      methods: {
          // go to specific location
          goToPath: function(path){
            this.$router.push({name: path})
          }
      }
    }
</script>

<style scoped>
  #home{
    height: 100vh;
  }
</style>
