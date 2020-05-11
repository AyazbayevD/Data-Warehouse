<template>
  <div>
    <form class="form-inline">
      <input class="form-control" v-model="search" placeholder="Search" aria-label="Search" style="width: 100%; text-align: center">
    </form>

    <table class="table">
      <thead>
      <tr>
        <th scope="col">#</th>
        <th scope="col">Person</th>
        <th scope="col">Role</th>
      </tr>
      </thead>
      <tbody>
      <tr @click="goToPerson(person)" v-for="(person, i) in filteredPeople">
        <th scope="row">{{i+1}}</th>
        <td>{{person.name}}</td>
        <td>Olympiad participant</td>
      </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
    export default {
        data(){
          return{
            people: [],
            search: ''
          }
        },

        created() {
          let people = [];
          fetch('src/1.json').then(function (resp) {
            return resp.json();
          }).then(function (data) {
            let names = data.first_name;
            let surnames = data.last_name;
            for(let i = 0; i < 60; i++){
              people.push({name: names[i] + " " + surnames[i]});
            }
          });

          this.people = people;
        },

        computed: {
          filteredPeople: function () {
            return this.people.filter((person) => {
              return person.name.match(this.search);
            });
          }
        },

        methods: {
          goToPerson(person){
            this.$router.push({name: 'Person', params: {name: person.name}});
          }
        }
    }
</script>

<style scoped>

</style>
