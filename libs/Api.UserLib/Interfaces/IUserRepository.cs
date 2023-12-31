using Api.ReferenceLib.Models;
using Api.UserLib.Models;

namespace Api.UserLib.Interfaces
{
    public interface IUserRepository
    {
        public Task<Response> AddUser(UserModel user);
        public Task<UserModel> FindUser(string userId);
        public Task<Response> AddUserAsync(UserModel user);
        public Task<Response> ReplaceUser(UserModel userModel);
    }
}